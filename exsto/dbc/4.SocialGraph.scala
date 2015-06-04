// Databricks notebook source exported at Thu, 4 Jun 2015 04:39:21 UTC
// MAGIC %md
// MAGIC # Construct a Social Graph of Sender/Replier

// COMMAND ----------

val nodes = sqlContext.parquetFile("/mnt/paco/exsto/graph/reply_node.parquet")
node.registerTempTable("node")

val edges = sqlContext.parquetFile("/mnt/paco/exsto/graph/reply_edge.parquet")
edge.registerTempTable("edge")

// COMMAND ----------

import org.apache.spark.graphx._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


// COMMAND ----------

val edgeRDD = edges.map{ p =>
  Edge(p(0).asInstanceOf[Long], p(1).asInstanceOf[Long], p(2).asInstanceOf[Int])
}.distinct()

// COMMAND ----------

val nodeRDD = nodes.map{ p =>
  (p(0).asInstanceOf[Long], p(1).asInstanceOf[String])
}.distinct()


// COMMAND ----------

val g: Graph[String, Int] = Graph(nodeRDD, edgeRDD)


// COMMAND ----------

// MAGIC %md
// MAGIC Now run *PageRank* on this graph to find the top-ranked email repliers

// COMMAND ----------

case class Rank (id: Long, rank: Double)

val rank_df = g.pageRank(0.0001).vertices.map(x => Rank(x._1.asInstanceOf[Long], x._2)).toDF()
rank_df.registerTempTable("rank")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT rank.rank, node.sender
// MAGIC FROM rank JOIN node ON (rank.id = node.id)
// MAGIC ORDER BY rank.rank DESC
// MAGIC LIMIT 20

// COMMAND ----------

// MAGIC %md
// MAGIC Let's get some metrics about the social graph...

// COMMAND ----------

import org.apache.spark.graphx.VertexId 

// define a reduce operation to compute the highest degree vertex
def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
  if (a._2 > b._2) a else b
}

// compute the max degrees
val maxInDegree: (VertexId, Int)  = g.inDegrees.reduce(max)
val maxOutDegree: (VertexId, Int) = g.outDegrees.reduce(max)
val maxDegrees: (VertexId, Int)   = g.degrees.reduce(max)

// COMMAND ----------

// connected components
val cc = g.stronglyConnectedComponents(100).vertices
cc.take(2)

// COMMAND ----------

case class Component (name: String, component: Long)

val cc_df = nodeRDD.join(cc).map {
  case (id, (name, cc)) => Component(name, cc)
}.toDF()

cc_df.registerTempTable("cc")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT component, COUNT(*) AS num
// MAGIC FROM cc
// MAGIC GROUP BY component
// MAGIC ORDER BY num DESC

// COMMAND ----------


