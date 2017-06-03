package cl.uchile.dcc.pmd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId

/**
 * @author ${user.name}
 */
object App {
  
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[1]")
    val sc = new SparkContext(conf)
    //val graph =  GraphLoader.edgeListFile(sc, "edges.csv")
    // Create an RDD for edges
    val relationships: RDD[Edge[VertexId]] =
      sc.parallelize(Array(Edge(0L, 4L, 0L),
        Edge(0L, 2L, 0L),
        Edge(0L, 1L, 0L),
        Edge(1L, 2L, 0L),
        Edge(1L, 3L, 0L),
        Edge(2L, 4L, 0L),
        Edge(2L, 3L, 0L),
        Edge(3L, 5L, 0L),
        Edge(3L, 6L, 0L),
        Edge(4L, 5L, 0L),
        Edge(5L, 6L, 0L),
        Edge(7L, 8L, 0L)))
    // Define a default user in case there are relationship with missing user
    val defaultUser = -1L
    // Build the initial Graph
    val graph = Graph.fromEdges(relationships, defaultUser)
    
    val dg = ComputeDegree.degree(graph)
    dg.vertices.collect.foreach{println(_)}
    
    /*val idg = ComputeDegree.inDegree(graph)
    idg.vertices.collect.foreach{println(_)}
        
    val odg = ComputeDegree.outDegree(graph)
    odg.vertices.collect.foreach{println(_)}
    
    val pr = graph.staticPageRank(20)
    pr.vertices.collect.foreach{println(_)}
    
    val c = ComputeCloseness.closeness(graph)
    c.vertices.collect.foreach{println(_)}
    
    val b = ComputeBetweenness.betweenness(graph)
    b.vertices.collect.foreach{println(_)}*/
  }

}
