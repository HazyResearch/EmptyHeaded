package duncecap

import org.scalatest.FunSuite
import org.apache.spark._
import org.apache.spark.graphx._


class GraphXTest extends FunSuite {
  val conf = new SparkConf().setAppName("GraphXTest").setMaster("local")
  val sc = new SparkContext(conf)

  val filename = sys.env("EMPTYHEADED_HOME") +
    "/test/graph/data/facebook_pruned.tsv"

  test("Triangle counting") {
    val start = System.nanoTime()

    val graph = GraphLoader.edgeListFile(
      sc, filename, canonicalOrientation = true
    ).partitionBy(PartitionStrategy.RandomVertexCut)

    val triCounts = graph.triangleCount()
    val result = (triCounts.vertices.map({
      case (_, count) => count
    }).sum() / 3).toInt

    val end = System.nanoTime()
    println(s"FOUND: ${result}")
    println(s"ELAPSED TIME: ${(end - start) / 1.0E9}s")
  }
}
