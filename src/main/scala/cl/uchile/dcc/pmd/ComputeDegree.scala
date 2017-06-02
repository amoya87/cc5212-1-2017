package cl.uchile.dcc.pmd

import org.apache.spark.graphx._
import scala.reflect.ClassTag

object ComputeDegree {
  
  def calculeGrade[VD, ED: ClassTag](gradeType: String, graph: Graph[VD, ED]): Graph[Double, ED] = {
    
    val spGraph =  graph.mapVertices{ (vid, attr) =>  0.0}
    var degrees =
    gradeType match {
      case "1" => spGraph.inDegrees.map(x => (x._1, x._2.asInstanceOf[Double]))
      case "2" => spGraph.outDegrees.map(x => (x._1, x._2.asInstanceOf[Double]))
      case "3" => spGraph.degrees.map(x => (x._1, x._2.asInstanceOf[Double]))
    }
    
    graph.outerJoinVertices(degrees)((vid, _, degOpt) => degOpt.getOrElse(0.0))
  }

  def inDegree[VD, ED: ClassTag](graph: Graph[VD, ED]): Graph[Double, ED] = {
    calculeGrade("1", graph)
  }
  
  def outDegree[VD, ED: ClassTag](graph: Graph[VD, ED]): Graph[Double, ED] = {
    calculeGrade("2", graph)
  }
  
  def degree[VD, ED: ClassTag](graph: Graph[VD, ED]): Graph[Double, ED] = {
    calculeGrade("3", graph)
  }

}