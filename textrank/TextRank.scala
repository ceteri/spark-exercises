import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext._

// build the graph

val word = sqlContext.parquetFile("word.parquet")
word.registerTempTable("word")

val edge = sqlContext.parquetFile("edge.parquet")
edge.registerTempTable("edge")

sql("SELECT * FROM word").take(5)
sql("SELECT * FROM edge").take(5)

val n = sql("SELECT id, stem FROM word").distinct()
val nodes: RDD[(Long, String)] = n.map(p => (p(0).asInstanceOf[Long], p(1).asInstanceOf[String]))

val e = sql("SELECT * FROM edge")
val edges: RDD[Edge[Int]] = e.map(p => Edge(p(0).asInstanceOf[Long], p(1).asInstanceOf[Long], 0))

// run PageRank

val g: Graph[String, Int] = Graph(nodes, edges)
val r = g.pageRank(0.0001).vertices

r.join(nodes).sortBy(_._2._1, ascending=false).foreach(println)

// save the ranks

case class Rank(id: Int, rank: Float)
val rank = r.map(p => Rank(p._1.toInt, p._2.toFloat))

rank.registerTempTable("rank")
rank.saveAsParquetFile("rank.parquet")

//////////////////////////////////////////////////////////////////////

def median[T](s: Seq[T])(implicit n: Fractional[T]) = {
  import n._
  val (lower, upper) = s.sortWith(_<_).splitAt(s.size / 2)
  if (s.size % 2 == 0) (lower.last + upper.head) / fromInt(2) else upper.head
}

node.schema
edge.schema
rank.schema

val s = sql("SELECT w.index, w.word, r.rank FROM word w JOIN rank r ON w.id = r.id ORDER BY w.index").collect()

val min_rank = median(r.map(_._2).collect())

var span:List[String] = List()
var last_index = -1
var rank_sum = 0.0

var phrases:collection.mutable.Map[String, Double] = collection.mutable.Map()

s.foreach { x => 
  //println (x)
  val index = x.getInt(0)
  val word = x.getString(1)
  val rank = x.getFloat(2)

  var isStop = false

  // test for break from past
  if (span.size > 0 && rank < min_rank) isStop = true
  if (span.size > 0 && (index - last_index > 1)) isStop = true

  // clear accumulation
  if (isStop) {
    val phrase = span.mkString(" ")
    phrases += (phrase -> rank_sum)
    //println(phrase, rank_sum)

    span = List()
    last_index = index
    rank_sum = 0.0
  }

  // start or append
  if (rank >= min_rank) {
    span = span :+ word
    last_index = index
    rank_sum += rank
  }
}

// summarize the text as a list of ranked keyphrases

var summary = sc.parallelize(phrases.toSeq).distinct().sortBy(_._2, ascending=false).cache()
val min_rank = median(summary.map(_._2).collect().toSeq)
summary = summary.filter(_._2 >= min_rank)

val sum = summary.map(_._2).reduce(_ + _)
summary = summary.map(x => (x._1, x._2 / sum))
summary.collect()
