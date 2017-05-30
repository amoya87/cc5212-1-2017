package cl.uchile.dcc.pmd

import org.apache.spark.graphx._
import scala.reflect.ClassTag

object ComputeDegree {
  def calculeGrade[VD, ED: ClassTag](gradeType: String, graph: Graph[Long, ED]): Graph[Double, ED] = {
    var degrees: RDD[(VertexId, Double)] = null

    gradeType match {
      case "1" => degrees = graph.inDegrees.map(x => (x._1, x._2.asInstanceOf[Double]))
      case "2" => degrees = graph.outDegrees.map(x => (x._1, x._2.asInstanceOf[Double]))
      case "3" => degrees = graph.degrees.map(x => (x._1, x._2.asInstanceOf[Double]))
    }

    graph.outerJoinVertices(degrees)((vid, _, degOpt) => degOpt.getOrElse(0.0))
  }

  def inDegree[VD, ED: ClassTag](gradeType: String, graph: Graph[VD, ED]): Graph[Double, ED] = {
    calculeGrade("1", graph)
  }
  
  def outDegree[VD, ED: ClassTag](gradeType: String, graph: Graph[VD, ED]): Graph[Double, ED] = {
    calculeGrade("2", graph)
  }
  
  def degree[VD, ED: ClassTag](gradeType: String, graph: Graph[VD, ED]): Graph[Double, ED] = {
    calculeGrade("3", graph)
  }

}