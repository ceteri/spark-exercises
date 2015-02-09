// Databricks notebook source exported at Mon, 9 Feb 2015 04:37:21 UTC
import org.apache.spark.graphx._

val edge = sqlContext.parquetFile("/mnt/paco/exsto/graph/graf_edge.parquet")
edge.registerTempTable("edge")

val node = sqlContext.parquetFile("/mnt/paco/exsto/graph/graf_node.parquet")
node.registerTempTable("node")

// COMMAND ----------

// MAGIC %md Let's pick one message as an example -- at scale we would parallelize this to run for all the messages.

// COMMAND ----------

val msg_id = "CA+B-+fyrBU1yGZAYJM_u=gnBVtzB=sXoBHkhmS-6L1n8K5Hhbw"

// COMMAND ----------

import org.apache.spark.rdd.RDD

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

// MAGIC %md Next, we run PageRank with this graph...

// COMMAND ----------

val g: Graph[String, Int] = Graph(nodes, edges)
val r = g.pageRank(0.0001).vertices

// COMMAND ----------

r.join(nodes).sortBy(_._2._1, ascending=false).collect()

// COMMAND ----------

// MAGIC %md Then save the resulting ranks for each word of interest...

// COMMAND ----------

case class Rank(id: Int, rank: Float, word: String)
val rank = r.join(nodes).map(p => Rank(p._1.toInt, p._2._1.asInstanceOf[Float], p._2._2))

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

val rankMap = rank.map(r => (r.id, r.rank)).collectAsMap()

// COMMAND ----------

def median[T](s: Seq[T])(implicit n: Fractional[T]) = {
  import n._
  val (lower, upper) = s.sortWith(_<_).splitAt(s.size / 2)
  if (s.size % 2 == 0) (lower.last + upper.head) / fromInt(2) else upper.head
}

// COMMAND ----------

val parsed = sqlContext.jsonFile("/mnt/paco/exsto/parsed/")
parsed.registerTempTable("parsed")

// COMMAND ----------

parsed.printSchema

// COMMAND ----------

node.printSchema

// COMMAND ----------

edge.printSchema

// COMMAND ----------

rank.printSchema

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT graf, tile, size, polr, subj
// MAGIC FROM parsed
// MAGIC WHERE id='CA+B-+fyrBU1yGZAYJM_u=gnBVtzB=sXoBHkhmS-6L1n8K5Hhbw'

// COMMAND ----------

val sql = """
SELECT num, node_id, raw, pos, keep
FROM node
WHERE id='%s'
""".format(msg_id)

val para = sqlContext.sql(sql)

// COMMAND ----------

para.collect()

// COMMAND ----------

var last_idx: Int = -1
var span: List[String] = List()
var rank_sum: Double = 0.0
var noun_count: Int = 0
var phrases: collection.mutable.Map[String, Double] = collection.mutable.Map()

para.collect().foreach{ x =>
  val w_idx = x(0).asInstanceOf[Int]
  val node_id = x(1).asInstanceOf[Long].toInt
  val word = x(2).asInstanceOf[String].toLowerCase()
  val pos = x(3).asInstanceOf[String]
  val keep = x(4).asInstanceOf[Int]
  
  if (keep == 1) {
    if (w_idx - last_idx > 1) {
      if (noun_count > 0) {
        phrases += (span.mkString(" ") -> rank_sum)
      }
      
      span = List()
      rank_sum = 0.0
      noun_count = 0
    }
  
    val rank = rankMap.get(node_id).getOrElse(0.0).asInstanceOf[Float]
    //println(w_idx, node_id, word, pos, rank)

    last_idx = w_idx
    span = span :+ word
    rank_sum += rank
    
    if (pos.startsWith("N")) noun_count += 1
  }
}

if (noun_count > 0) {
  phrases += (span.mkString(" ") -> rank_sum)
}


// COMMAND ----------

case class Phrase(phrase: String, norm_rank: Double)

val phraseRdd = sc.parallelize(phrases.toSeq).distinct()
val sum = phraseRdd.map(_._2).reduce(_ + _)

val phraseTable = phraseRdd.map(p => Phrase(p._1, p._2 / sum))
phraseTable.registerTempTable("phrase")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM phrase
// MAGIC ORDER BY norm_rank DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT text
// MAGIC FROM msg
// MAGIC WHERE id='CA+B-+fyrBU1yGZAYJM_u=gnBVtzB=sXoBHkhmS-6L1n8K5Hhbw'

// COMMAND ----------

// MAGIC %md Just for kicks, let's compare the results of **TextRank** with the *term frequencies* that would result from a **WordCount** ...

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
// MAGIC That's why you probably want to enrich text analytics results before other algorithms need to use them downstream.
