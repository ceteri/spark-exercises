// SSSP impl in Graphx using Pregel
//https://en.wikipedia.org/wiki/Dijkstra%27s_algorithm
//http://spark.apache.org/docs/latest/graphx-programming-guide.html#pregel-api
//http://stackoverflow.com/questions/23700124/how-to-get-sssp-actual-path-by-apache-spark-graphx

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators

val graph = GraphGenerators.logNormalGraph(sc, numVertices = 5, numEParts = sc.defaultParallelism, mu = 4.0, sigma = 1.3).mapEdges(e => e.attr.toDouble)
graph.edges.foreach(println)

// initialize all vertices except the root to have distance infinity
val sourceId: VertexId = 0
val initialGraph : Graph[(Double, List[VertexId]), Double] = graph.mapVertices((id, _) => if (id == sourceId) (0.0, List[VertexId](sourceId)) else (Double.PositiveInfinity, List[VertexId]()))

val sssp = initialGraph.pregel((Double.PositiveInfinity, List[VertexId]()), Int.MaxValue, EdgeDirection.Out)(
  // vertex program
  (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist, 

  // send message
  triplet => {
    if (triplet.srcAttr._1 < triplet.dstAttr._1 - triplet.attr ) {
      Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr , triplet.srcAttr._2 :+ triplet.dstId)))
    } else {
      Iterator.empty
    }
  },

  // merge message
  (a, b) => if (a._1 < b._1) a else b)

  println(sssp.vertices.collect.mkString("\n")
)
