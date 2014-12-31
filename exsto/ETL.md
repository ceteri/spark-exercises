## ETL in PySpark with Spark SQL

Let's use PySpark and Spark SQL to prepare the data for ML and graph
analysis.
We can perform *data discovery* while reshaping the data for later
work.
These early results can help guide our deeper analysis.

NB: if this ETL needs to run outside of the `bin/pyspark` shell, first
set up a `SparkContext` variable:

```python
from pyspark import SparkContext
sc = SparkContext(appName="Exsto", master="local[*]")
```

Import the JSON data produced by the scraper and register its schema
for ad-hoc SQL queries later.
Each message has the fields: 
`date`, `sender`, `id`, `next_thread`, `prev_thread`, `next_url`, `subject`, `text`

```python
from pyspark.sql import SQLContext, Row
sqlCtx = SQLContext(sc)

msg = sqlCtx.jsonFile("data").cache()
msg.registerTempTable("msg")
```

NB: note the persistence used for the JSON message data.
We may need to unpersist at a later stage of this ETL work.

### Question: Who are the senders?

Who are the people in the developer community sending email to the list?
We will use this as a dimension in our analysis and reporting.
Let's create a map, with a unique ID for each email address --
this will be required for the graph analysis.
It may come in handy later for some
[named-entity recognition](https://en.wikipedia.org/wiki/Named-entity_recognition).

```python
who = msg.map(lambda x: x.sender).distinct().zipWithUniqueId()
who.take(10)

whoMap = who.collectAsMap()
```

### Question: Who are the top K senders?

[Apache Spark](http://spark.apache.org/) is one of the most
active open source developer communities on Apache, so it
will tend to have several thousand people engaged.
Let's identify the most active ones.
Then we can show a leaderboard and track changes in it over time.

```python
from operator import add

top_sender = msg.map(lambda x: (x.sender, 1,)).reduceByKey(add) \
 .map(lambda (a, b): (b, a)) \
 .sortByKey(0, 1) \
 .map(lambda (a, b): (b, a))

top_sender.take(11)
```

You many notice that code... it comes from *word count*.


### Question: Which are the top K conversations?

Clearly, some people discuss over the email list more than others.
Let's identify *who* those people are.
Later we can leverage our graph analysis to determine *what* they discuss.

NB: note the use case for `groupByKey` transformations; 
sometimes its usage is indicated.

```python
import itertools

def nitems (replier, senders):
  for sender, g in itertools.groupby(senders):
    yield len(list(g)), (replier, sender,)

senders = msg.map(lambda x: (x.id, x.sender,))
replies = msg.map(lambda x: (x.prev_thread, x.sender,))

convo = replies.join(senders).values() \
 .filter(lambda (a, b): a != b)

top_convo = convo.groupByKey() \
 .flatMap(lambda (a, b): list(nitems(a, b))) \
 .sortByKey(0)

top_convo.take(10)
```

### Prepare for Sender/Reply Graph Analysis

Given the RDDs that we have created to help answer some of the
questions so far, let's persist those data sets using
[Parquet](http://parquet.io) --
starting with the graph of sender/message/reply:

```python
edge = top_convo.map(lambda (a, b): (whoMap.get(b[0]), whoMap.get(b[1]), a,))
edgeSchema = edge.map(lambda p: Row(replier=p[0], sender=p[1], count=int(p[2])))
edgeTable = sqlCtx.inferSchema(edgeSchema)
edgeTable.saveAsParquetFile("reply_edge.parquet")

node = who.map(lambda (a, b): (b, a))
nodeSchema = node.map(lambda p: Row(id=int(p[0]), sender=p[1]))
nodeTable = sqlCtx.inferSchema(nodeSchema)
nodeTable.saveAsParquetFile("reply_node.parquet")
```

---

*(TBD)*

Parse the text in email messages.
NB: FIX THIS (only one elem now)

```python
import exsto

def parse_text (id, text):
  for graf_text in exsto.filter_quotes(text):
    yield id, exsto.parse_graf(graf_text)[0]

msg1 = sc.parallelize(msg.take(1))
grafs = msg1.flatMap(lambda x: list(parse_text(x.id, x.text)))
grafs.collect()
```

Each paragraph has a [SHA-1](https://en.wikipedia.org/wiki/SHA-1)
digest of its parsed sentences, effectively making parts of our
semantic analysis *content addressable*.

Store a mapping of `(ID, SHA1)` to preserve the composition of each
message.

```python
sha1 = grafs.map(lambda (id, meta): (id, meta["sha1"],))
sha1Schema = sha1.map(lambda p: Row(id=p[0], sha1=p[1]))
sha1Table = sqlCtx.inferSchema(sha1Schema)
sha1Table.saveAsParquetFile("id_sha1.parquet")
```

Next, store a mapping of `(SHA1, parsed_graf)` for each paragraph --
so that we have a tree structure for the parse data.
This is useful to detect duplicated paragraphs and thus avoid
distorting the analytics.

```python
def csv_tiles (tiles):
  return "\t".join(["%d,%d" % (n0, n1) for n0, n1 in tiles])

def word_tuple (t):
  id, word, lemma, pos, keep, idx = t
  return ",".join([str(id), word, lemma, pos, str(keep), str(idx)])

def csv_words (words):
  return "\t".join([word_tuple(t) for t in words])

text = grafs.map(lambda (id, m): (m["sha1"], csv_tiles(m["tile"]), csv_words(m["graf"]), m["polr"], m["subj"],))

textSchema = text.map(lambda p: Row(sha1=p[0], tiles=p[1], words=p[2], polr=float(p[3]), subj=float(p[4])))
textTable = sqlCtx.inferSchema(textSchema)
textTable.saveAsParquetFile("sha1_graf.parquet")
```
