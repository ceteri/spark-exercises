# Databricks notebook source exported at Thu, 4 Jun 2015 02:03:35 UTC
# MAGIC %md
# MAGIC ## ETL in PySpark with Spark SQL
# MAGIC 
# MAGIC Let's use PySpark and Spark SQL to prepare the data for ML and graph
# MAGIC analysis.
# MAGIC We can perform *data discovery* while reshaping the data for later
# MAGIC work.
# MAGIC These early results can help guide our deeper analysis.
# MAGIC 
# MAGIC See also: overview of how to use this data in 
# MAGIC [Exsto: ETL in PySpark with Spark SQL](https://github.com/ceteri/spark-exercises/blob/master/exsto/ETL.md)

# COMMAND ----------

# MAGIC %md
# MAGIC Import the JSON data produced by the scraper and register its schema
# MAGIC for ad-hoc SQL queries later.
# MAGIC Each message has the fields:
# MAGIC `date`, `sender`, `id`, `next_thread`, `prev_thread`, `next_url`, `subject`, `text`

# COMMAND ----------

msg = sqlContext.jsonFile("/mnt/paco/exsto/original/").cache()
msg.registerTempTable("msg")
msg.count()

# COMMAND ----------

# MAGIC %md
# MAGIC NB: persistence gets used to cache the JSON message data.
# MAGIC We may need to unpersist at a later stage of this ETL work.

# COMMAND ----------

msg.first()

# COMMAND ----------

msg.printSchema()

# COMMAND ----------

# MAGIC %md ### Question: Who are the senders?
# MAGIC 
# MAGIC Who are the people in the developer community sending email to the list?
# MAGIC We will use this repeatedly as a dimension in our analysis and reporting.
# MAGIC 
# MAGIC Let's create a map, with a unique ID for each email address -- along with an inverse lookup.
# MAGIC This will be required for the graph analysis later.
# MAGIC It may also come in handy for resolving some
# MAGIC [named-entity recognition](https://en.wikipedia.org/wiki/Named-entity_recognition)
# MAGIC issues, i.e., cleaning up the data where people may be using multiple email addresses.
# MAGIC 
# MAGIC Note that we use that map as a [broadcast variable](http://spark.apache.org/docs/latest/programming-guide.html#broadcast-variables).

# COMMAND ----------

who = msg.map(lambda x: x.sender).distinct().zipWithUniqueId()
who_dict = who.collectAsMap()

whoMap = sc.broadcast(who_dict)
whoInv = sc.broadcast({v: k for k, v in who_dict.items()})

print "senders:", len(whoMap.value)
print whoMap.value

# COMMAND ----------

# MAGIC %md ### Question: Who are the top K senders?
# MAGIC 
# MAGIC [Apache Spark](http://spark.apache.org/) is one of the most
# MAGIC active open source developer communities on Apache, so it
# MAGIC will tend to have several thousand people engaged.
# MAGIC 
# MAGIC Let's identify the most active ones.
# MAGIC Then we can show a leaderboard and track changes in it over time.

# COMMAND ----------

from operator import add

top_sender = msg.map(lambda x: (x.sender, 1,)).reduceByKey(add) \
 .map(lambda (a, b): (b, a)) \
 .sortByKey(0, 1) \
 .map(lambda (a, b): (b, a))

print "top senders:", top_sender.take(11)

# COMMAND ----------

# MAGIC %md
# MAGIC Did you notice anything familiar about that code?
# MAGIC It comes from _word count_.
# MAGIC 
# MAGIC Alternatively, let's take a look at how to create that leaderboard using SQL...

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT sender, COUNT(id) AS msg_count, MIN(date) AS earliest, MAX(date) AS latest
# MAGIC FROM msg 
# MAGIC GROUP BY sender 
# MAGIC ORDER BY msg_count DESC
# MAGIC LIMIT 25

# COMMAND ----------

# MAGIC %md It would be interesting to break that down a bit, and see how the *count* of messages sent compares with the *duration* of time in which the sender was engaged with the email list...

# COMMAND ----------

from dateutil import parser
from pyspark.sql import Row
from pyspark.sql.types import *

def days_hours_minutes (td):
    return float(td.days) + float(td.seconds) / 3600 + (float(td.seconds) / 60) % 60

sql = """
SELECT sender, COUNT(id) AS msg_count, MIN(date) AS earliest, MAX(date) AS latest
FROM msg 
GROUP BY sender 
"""

leaders = sqlContext.sql(sql) \
 .map(lambda x: (x[0], int(x[1]), days_hours_minutes(parser.parse(x[3]) - parser.parse(x[2]))))
  
fields = [StructField("sender", StringType(), True), StructField("count", IntegerType(), True), StructField("duration", FloatType(), True)]
schema = StructType(fields)

leadTable = sqlContext.createDataFrame(leaders, schema)
leadTable.registerTempTable("leaders")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sender, count, duration
# MAGIC FROM leaders
# MAGIC ORDER BY count DESC
# MAGIC LIMIT 30

# COMMAND ----------

# MAGIC %md Let's try to learn more about the structure of relationships among the people conversing on the list...

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT c0.subject, c0.sender, c1.sender AS receiver
# MAGIC FROM msg c0 JOIN msg c1 ON c0.id = c1.prev_thread
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md Sometimes people answer their own messages...

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT c0.subject, c0.sender, c0.id
# MAGIC FROM msg c0 JOIN msg c1 ON c0.id = c1.prev_thread
# MAGIC WHERE c0.sender = c1.sender
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT COUNT(*)
# MAGIC FROM msg c0 JOIN msg c1 ON c0.id = c1.prev_thread
# MAGIC WHERE c0.sender = c1.sender

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT COUNT(c0.subject) AS num, c0.sender AS sender
# MAGIC FROM msg c0 JOIN msg c1 ON c0.id = c1.prev_thread
# MAGIC WHERE c0.sender = c1.sender
# MAGIC GROUP BY c0.sender
# MAGIC ORDER BY num DESC

# COMMAND ----------

# MAGIC %md ### Question: Which are the top K conversations?
# MAGIC 
# MAGIC Clearly, some people discuss over the email list more than others.
# MAGIC 
# MAGIC Let's identify *who* those people are.
# MAGIC We can also determine who they in turn discuss with the most.
# MAGIC Later we can leverage our graph analysis to determine *what* they discuss.
# MAGIC 
# MAGIC Here is a great place to make use of our `whoMap` broadcast variable, since it's better to be sorting integers at scale than to need to sort many strings.
# MAGIC 
# MAGIC Note the use case for the [groupByKey](http://spark.apache.org/docs/latest/programming-guide.html#transformations) transformation.
# MAGIC Generally we [prefer to avoid it]((http://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html), but this is a good illustration of where its usage is indicated.

# COMMAND ----------

import itertools

senders = msg.map(lambda x: (x.id, whoMap.value.get(x.sender),)).distinct()
replies = msg.map(lambda x: (x.prev_thread, whoMap.value.get(x.sender),)).distinct()

convo = replies.join(senders).values() \
 .filter(lambda (a, b): a != b)

def nitems (replier, senders):
  for sender, g in itertools.groupby(senders):
    yield len(list(g)), (replier, sender,)

# COMMAND ----------

top_convo = convo.groupByKey() \
 .flatMap(lambda (a, b): list(nitems(a, b))) \
 .sortByKey(0)

print "top convo", top_convo.take(10)

# COMMAND ----------

conv = top_convo.map(lambda p: (p[0], whoInv.value.get(p[1][0]), whoInv.value.get(p[1][1]),))

fields = [StructField("count", IntegerType(), True), StructField("sender", StringType(), True), StructField("replier", StringType(), True)]
schema = StructType(fields)

convTable = sqlContext.createDataFrame(conv, schema)
convTable.registerTempTable("conv")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count, sender, replier
# MAGIC FROM conv
# MAGIC ORDER BY count DESC

# COMMAND ----------

# MAGIC %md Just curious... how many "dead end" threads during that period? In other words, how many messages that had no replies or where the senders answered themselves?

# COMMAND ----------

msg.count() - conv.map(lambda x: x[0]).sum()

# COMMAND ----------

# MAGIC %md ### Prepare for Sender/Reply Graph Analysis
# MAGIC 
# MAGIC Given the RDDs that we have created to help answer some of the
# MAGIC questions so far, let's persist those data sets using
# MAGIC [Parquet](http://parquet.io) --
# MAGIC starting with the graph of sender/message/reply:

# COMMAND ----------

dbutils.fs.rm("/mnt/paco/exsto/graph/reply_edge.parquet", True)
dbutils.fs.rm("/mnt/paco/exsto/graph/reply_node.parquet", True)

# COMMAND ----------

edge = top_convo.map(lambda (a, b): (long(b[0]), long(b[1]), a,))

fields = [StructField("replier", LongType(), True), StructField("sender", LongType(), True), StructField("count", IntegerType(), True)]
schema = StructType(fields)

edgeTable = sqlContext.createDataFrame(edge, schema)
edgeTable.saveAsParquetFile("/mnt/paco/exsto/graph/reply_edge.parquet")

node = who.map(lambda (a, b): (long(b), a))

fields = [StructField("id", LongType(), True), StructField("sender", StringType(), True)]
schema = StructType(fields)

nodeTable = sqlContext.createDataFrame(node, schema)
nodeTable.saveAsParquetFile("/mnt/paco/exsto/graph/reply_node.parquet")


# COMMAND ----------

node.take(2)


# COMMAND ----------

# MAGIC %md ### Prepare for TextRank Analysis per paragraph
# MAGIC 
# MAGIC We will load this as text, not as JSON, as a convenient way to parse the nested tuples.

# COMMAND ----------

graf = sc.textFile("/mnt/paco/exsto/parsed/").cache()
graf.first()

# COMMAND ----------

import json

def map_graf_edges (x):
  j = json.loads(x)

  for pair in j["tile"]:
    n0 = long(pair[0])
    n1 = long(pair[1])

    if n0 > 0 and n1 > 0:
      yield (j["id"], n0, n1,)
      yield (j["id"], n1, n0,)

grafEdge = graf.flatMap(map_graf_edges)

print "graf edges", grafEdge.count()

# COMMAND ----------

grafEdge.take(5)

# COMMAND ----------

def map_graf_nodes (x):
  j = json.loads(x)

  for word in j["graf"]:
    yield [j["id"]] + word

grafNode = graf.flatMap(map_graf_nodes)

print "graf nodes", grafNode.count()

# COMMAND ----------

grafNode.take(5)

# COMMAND ----------

dbutils.fs.rm("/mnt/paco/exsto/graph/graf_edge.parquet", True)
dbutils.fs.rm("/mnt/paco/exsto/graph/graf_node.parquet", True)

# COMMAND ----------

fields = [StructField("id", StringType(), True), StructField("node0", LongType(), True), StructField("node1", LongType(), True)]
schema = StructType(fields)

grafEdgeTable = sqlContext.createDataFrame(grafEdge, schema)
grafEdgeTable.saveAsParquetFile("/mnt/paco/exsto/graph/graf_edge.parquet")

# COMMAND ----------

fields = [StructField("id", StringType(), True), StructField("node_id", LongType(), True), StructField("raw", StringType(), True), StructField("root", StringType(), True), StructField("pos", StringType(), True), StructField("keep", IntegerType(), True), StructField("num", IntegerType(), True)]
schema = StructType(fields)

grafNodeTable = sqlContext.createDataFrame(grafNode, schema)
grafNodeTable.saveAsParquetFile("/mnt/paco/exsto/graph/graf_node.parquet")

# COMMAND ----------


