package cl.uchile.dcc.pmd

import org.apache.spark.graphx._
import scala.reflect.ClassTag

object ComputeCloseness {
  
  def closeness[VD, ED: ClassTag](graph: Graph[VD, ED]): Graph[Double, ED] = {

    val shortestPaths = MyShortestPaths.run(graph)
    
    shortestPaths.mapVertices((_, attr) => func(attr))
  }

  def func(v: Map[Long, Int]): Double = {

    val sum = v.foldLeft(0.0)(_ + 1.0 / _._2)

    return sum
  }

}