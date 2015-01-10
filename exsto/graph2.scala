import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

val sqlCtx = new org.apache.spark.sql.SQLContext(sc)
import sqlCtx._

val edge = sqlCtx.parquetFile("reply_edge.parquet")
edge.registerTempTable("edge")

val node = sqlCtx.parquetFile("reply_node.parquet")
node.registerTempTable("node")

edge.schemaString
node.schemaString


val sql = "SELECT id, sender FROM node"

val n = sqlCtx.sql(sql).distinct()
val nodes: RDD[(Long, String)] = n.map{ p =>
  (p(0).asInstanceOf[Long], p(1).asInstanceOf[String])
}
nodes.collect()


val sql = "SELECT replier, sender, num FROM edge"

val e = sqlCtx.sql(sql).distinct()
val edges: RDD[Edge[Int]] = e.map{ p =>
  Edge(p(0).asInstanceOf[Long], p(1).asInstanceOf[Long], p(2).asInstanceOf[Int])
}
edges.collect()


// run graph analytics

val g: Graph[String, Int] = Graph(nodes, edges)
val r = g.pageRank(0.0001).vertices

r.join(nodes).sortBy(_._2._1, ascending=false).foreach(println)

// define a reduce operation to compute the highest degree vertex

def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
  if (a._2 > b._2) a else b
}

// compute the max degrees

val maxInDegree: (VertexId, Int)  = g.inDegrees.reduce(max)
val maxOutDegree: (VertexId, Int) = g.outDegrees.reduce(max)
val maxDegrees: (VertexId, Int)   = g.degrees.reduce(max)

val node_map: scala.collection.Map[Long, String] = node.
  map(p => (p(0).asInstanceOf[Long], p(1).asInstanceOf[String])).collectAsMap()

// connected components

val scc = g.stronglyConnectedComponents(10).vertices
node.join(scc).foreach(println)
