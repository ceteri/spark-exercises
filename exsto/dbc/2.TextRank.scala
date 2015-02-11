// Databricks notebook source exported at Mon, 9 Feb 2015 11:25:45 UTC
// MAGIC %md ## Augmented TextRank in Spark
// MAGIC The following is a 
// MAGIC [Spark](http://spark.apache.org/)
// MAGIC implementation of 
// MAGIC [TextRank](http://web.eecs.umich.edu/~mihalcea/papers/mihalcea.emnlp04.pdf)
// MAGIC by Mihalcea, et al.
// MAGIC The graph used in the algorithm is enriched by replacing the original authors' *Porter stemmer* approach with lemmatization from 
// MAGIC [WordNet](http://wordnet.princeton.edu/).
// MAGIC 
// MAGIC This algorithm generates a *graph* from a text document, linking together related words, then runs 
// MAGIC [PageRank](http://en.wikipedia.org/wiki/PageRank) 
// MAGIC on that graph to determine the high-ranked keyphrases.,
// MAGIC Those keyphrases summarize the text document, similar to how an human editor would summarize for an academic paper.
// MAGIC 
// MAGIC See [https://github.com/ceteri/spark-exercises/tree/master/exsto](https://github.com/ceteri/spark-exercises/tree/master/exsto)
// MAGIC and also the earlier [Hadoop implementation](https://github.com/ceteri/textrank)
// MAGIC which leveraged *semantic relations* by extending the graph using 
// MAGIC [hypernyms](https://en.wikipedia.org/wiki/Hyponymy_and_hypernymy) from WordNet as well.
// MAGIC 
// MAGIC First, we need to create *base RDDs* from the Parquet files that we stored in DBFS during the ETL phase...

// COMMAND ----------

val edge = sqlContext.parquetFile("/mnt/paco/exsto/graph/graf_edge.parquet")
edge.registerTempTable("edge")

val node = sqlContext.parquetFile("/mnt/paco/exsto/graph/graf_node.parquet")
node.registerTempTable("node")

// COMMAND ----------

// MAGIC %md Let's pick one message as an example -- at scale we would parallelize this to run for all the messages.

// COMMAND ----------

val msg_id = "CA+B-+fyrBU1yGZAYJM_u=gnBVtzB=sXoBHkhmS-6L1n8K5Hhbw"

// COMMAND ----------

// MAGIC %md Our use of [GraphX](https://spark.apache.org/graphx/) requires some imports...

// COMMAND ----------

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

// COMMAND ----------

// MAGIC %md Next we run a query in [Spark SQL](https://spark.apache.org/sql/) to deserialize just the fields that we need from the Parquet files to generate the graph nodes...

// COMMAND ----------

// use in parallelized version

val sql = """
SELECT node_id, root 
FROM node 
WHERE id='%s' AND keep=1
""".format(msg_id)

val n = sqlContext.sql(sql.stripMargin).distinct()

val nodes: RDD[(Long, String)] = n.map{ p =>
  (p(0).asInstanceOf[Long], p(1).asInstanceOf[String])
}

nodes.collect()

// COMMAND ----------

// MAGIC %md Likewise for the edges in the graph...

// COMMAND ----------

// use in parallelized version

val sql = """
SELECT node0, node1 
FROM edge 
WHERE id='%s'
""".format(msg_id)

val e = sqlContext.sql(sql.stripMargin).distinct()

val edges: RDD[Edge[Int]] = e.map{ p =>
  Edge(p(0).asInstanceOf[Long], p(1).asInstanceOf[Long], 0)
}

edges.collect()

// COMMAND ----------

// MAGIC %md ### Graph Analytics
// MAGIC We compose a graph from the `node` and `edge` RDDs and run [PageRank](http://spark.apache.org/docs/latest/graphx-programming-guide.html#pagerank) on it...

// COMMAND ----------

// use in parallelized version

val g: Graph[String, Int] = Graph(nodes, edges)
val r = g.pageRank(0.0001).vertices

// COMMAND ----------

// MAGIC %md Save the resulting ranks for each word of interest...

// COMMAND ----------

// use in parallelized version

case class Rank(id: Int, rank: Double, word: String)

val rank = r.join(nodes).map {
  case (node_id, (rank, word)) => Rank(node_id.toInt, rank, word)
}

rank.registerTempTable("rank")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT word, id, rank
// MAGIC FROM rank
// MAGIC ORDER BY rank DESC

// COMMAND ----------

// MAGIC %md
// MAGIC Okay, from purely a keyword perspective we've got some rankings. However, to make these useful, we need to go back to the sequence of the words in the text and pull out the top-ranked phrases overall.  We'll create `rankMap` to use for that...

// COMMAND ----------

// use in parallelized version

val rankMap = rank.map(r => (r.id, r.rank)).collectAsMap()

// COMMAND ----------

// MAGIC %md ### Email Summarization: Extracting Key Phrases
// MAGIC Next we got back to the parsed text and use the *TextRank* rankings to extract key phrases for each email message.
// MAGIC 
// MAGIC First, let's examing the parsed text for the example message...

// COMMAND ----------

val parsed = sqlContext.jsonFile("/mnt/paco/exsto/parsed/")
parsed.registerTempTable("parsed")

// COMMAND ----------

parsed.printSchema

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT graf, tile, size, polr, subj
// MAGIC FROM parsed
// MAGIC WHERE id='CA+B-+fyrBU1yGZAYJM_u=gnBVtzB=sXoBHkhmS-6L1n8K5Hhbw'

// COMMAND ----------

// MAGIC %md Fortunately, the stored Parquet files have that data available in an efficient way...

// COMMAND ----------

node.printSchema

// COMMAND ----------

// use in parallelized version

val sql = """
SELECT num, node_id, raw, pos, keep
FROM node
WHERE id='%s'
ORDER BY num ASC
""".format(msg_id)

val para = sqlContext.sql(sql)

// COMMAND ----------

// MAGIC %md The parsed text for the given message looks like the following sequence...

// COMMAND ----------

// use in parallelized version

val paraSeq = para
 .map(r => (r(0).asInstanceOf[Int], r(1).asInstanceOf[Long], r(2).toString, r(3).toString, r(4).asInstanceOf[Int]))
 .collect
 .toSeq

// COMMAND ----------

// MAGIC %md We define a function to extract key phrases from that sequence...

// COMMAND ----------

// use in parallelized version

def extractPhrases (s: Seq[(Int, Long, String, String, Int)]): Seq[(String, Double)] = {
  var last_idx: Int = -1
  var span: List[String] = List()
  var rank_sum: Double = 0.0
  var noun_count: Int = 0
  var phrases: collection.mutable.Map[String, Double] = collection.mutable.Map()

  s.foreach { row =>
    val(w_idx, node_id, word, pos, keep) = row

    if (keep == 1) {
      if (w_idx - last_idx > 1) {
        if (noun_count > 0) phrases += (span.mkString(" ").toLowerCase() -> rank_sum)

        span = List()
        rank_sum = 0.0
        noun_count = 0
      }
  
      val rank = rankMap.get(node_id.toInt).getOrElse(0.0).asInstanceOf[Float]
      //println(w_idx, node_id, word, pos, rank)

      last_idx = w_idx
      span = span :+ word
      rank_sum += rank

      if (pos.startsWith("N")) noun_count += 1
    }
  }

  if (noun_count > 0) phrases += (span.mkString(" ").toLowerCase() -> rank_sum)

  // normalize the ranks
  val sum = phrases.values.reduceLeft[Double](_ + _)
  val norm_ranks: collection.mutable.Map[String, Double] = collection.mutable.Map()

  phrases foreach {case (phrase, rank) => norm_ranks += (phrase -> rank / sum)}
  norm_ranks.toSeq
}

// COMMAND ----------

// MAGIC %md Now let's create an RDD from the extracted phrases and use SQL to show the results...

// COMMAND ----------

case class Phrase(phrase: String, norm_rank: Double)

val phraseRdd = sc.parallelize(extractPhrases(paraSeq)).map(p => Phrase(p._1, p._2))
phraseRdd.registerTempTable("phrase")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM phrase
// MAGIC ORDER BY norm_rank DESC

// COMMAND ----------

// MAGIC %md ### Evaluation
// MAGIC How do those results compare with what a human reader might extract from the message text?

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT text
// MAGIC FROM msg
// MAGIC WHERE id='CA+B-+fyrBU1yGZAYJM_u=gnBVtzB=sXoBHkhmS-6L1n8K5Hhbw'

// COMMAND ----------

// MAGIC %md Just for kicks, let's compare the results of *TextRank* with the *term frequencies* that would result from a **WordCount** ...

// COMMAND ----------

para.map(x => (x(2).asInstanceOf[String].toLowerCase(), 1))
  .reduceByKey(_ + _)
  .map(item => item.swap)
  .sortByKey(false, 1)
  .map(item => item.swap)
  .collect
  .foreach(println)

// COMMAND ----------

// MAGIC %md Um, yeah. So that happened.
// MAGIC 
// MAGIC That's why you probably want to clean-up and enrich the results of text analytics before other algorithms consume them downstream as *features*.
// MAGIC Otherwise, `GIGO` as they say.

// COMMAND ----------

// MAGIC %md ### Parallelized Version
// MAGIC 
// MAGIC Let's pull all of these pieces together into a function that can be run in parallel at scale...

// COMMAND ----------

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

case class Rank(id: Int, rank: Double, word: String)

def textRank (msg_id: String): Seq[(String, Double)] = {
  var sql = s"SELECT node_id, root FROM node WHERE id='%s' AND keep=1".format(msg_id)

  val nodes: RDD[(Long, String)] = sqlContext.sql(sql).distinct()
    .map{ p =>
      (p(0).asInstanceOf[Long], p(1).asInstanceOf[String])
    }

  sql = s"SELECT node0, node1 FROM edge WHERE id='%s'".format(msg_id)

  val edges: RDD[Edge[Int]] = sqlContext.sql(sql).distinct()
    .map{ p =>
      Edge(p(0).asInstanceOf[Long], p(1).asInstanceOf[Long], 0)
    }

  val g: Graph[String, Int] = Graph(nodes, edges)

  val rankMap = g.pageRank(0.0001).vertices
    .join(nodes)
    .map {
      case (node_id, (rank, word)) => (node_id.toInt, rank)
    }.collectAsMap()

  sql = s"SELECT num, node_id, raw, pos, keep FROM node WHERE id='%s' ORDER BY num ASC".format(msg_id)

  val paraSeq = sqlContext.sql(sql)
   .map(r => (r(0).asInstanceOf[Int], r(1).asInstanceOf[Long], r(2).toString, r(3).toString, r(4).asInstanceOf[Int]))
   .collect
   .toSeq

  extractPhrases(paraSeq)
}
