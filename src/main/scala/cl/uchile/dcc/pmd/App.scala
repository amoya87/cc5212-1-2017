package cl.uchile.dcc.pmd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.GraphLoader
import com.centrality.kBC.KBetweenness
import org.apache.hadoop.fs.{ FileSystem, FileUtil, Path }
import org.apache.hadoop.conf.Configuration

/**
 * @author ${user.name}
 */
object App {
  
  def main(args: Array[String]) {
    
    System.out.println(args.length)

    if (args.length < 2) {
      println("Usage: [edgesInputfile] [nodesInputfile] [outputfile]")
      //exit(1)
    }
    val master = "local[*]";
    val edgesInputfile = args(0)
    val nodesInputfile = args(1)
    val outputfile = args(2)
    val conf = new SparkConf().setAppName(App.getClass.getName()).setMaster(master)
    val sc = new SparkContext(conf)

    val spark =  SparkSession
      .builder()
      .appName(App.getClass.getName())
      .config("spark.master", "local")
      .getOrCreate();
    
    val rawDataEdges = sc.textFile(edgesInputfile)
    val rawDataNodes = sc.textFile(nodesInputfile)
    
    def convertToEdges(line: String)={
      val txt=line.split(",")
      new Edge(txt(0).toLong, txt(1).toLong, 0L)
    }  
     
    val edges:RDD[Edge[VertexId]] = rawDataEdges.map(x=> convertToEdges(x))
    
    val nodes: RDD[(VertexId,Any)]= rawDataNodes.map(x=> x.split(",")).map(y=> (y(0).toLong, y(1)+","+ y(2)))
        

    
    //val relationships:RDD[Edge[VertexId]]=arcsCache.map(x=>new Edge(x.get(0).asInstanceOf[Number].longValue, x.get(1).asInstanceOf[Number].longValue, 0L))
    
    
    val edgesC=edges.cache()
    
    edgesC.collect().foreach(println(_))    
    nodes.collect().foreach(println(_)) 
    // Build the initial Graph
    
    // Define a default user in case there are relationship with missing user
    val defaultUser = -1L
    
    val graph = Graph(nodes, edges, defaultUser)
    
    val graphCache=graph.cache()
    
    val numNodes=graphCache.numVertices
    val numEdges=graphCache.numEdges
 
    System.out.println("num nodos"+ numNodes)
    System.out.println("num edges"+ numEdges)
   
    //System.out.println("Closeness------>")
    //val c = ComputeCloseness.closeness(graphCache)    
    //val closeMap= c.vertices.map(x=>(x._1.toLong,x._2.toString))
    //c.triplets.saveAsTextFile(outputfile)    
    //val closeMapCache=closeMap.cache()
    
    System.out.println("Grado------>")
    val dg = ComputeDegree.degree(graphCache)   
    val dgMap= dg.vertices.map(x=>(x._1.toLong,x._2.toString))
    val dgMapCache=dgMap.cache()
    
    System.out.println("Betweetness------>")
    val kb = KBetweenness.run(graphCache,3)   
    val kbMap= kb.vertices.map(x=>(x._1.toLong,x._2.toString))    
    val kbMapCache=kbMap.cache()    
    
    
    System.out.println("Nodos------>")
    val nodesMap= nodes.map(x=> (x._1,x._2))
    //val joinMaps=nodesMap.join(dgMapCache).join(closeMapCache).join(kbMapCache)
    val joinMaps=nodesMap.join(dgMapCache).join(kbMapCache)
    //val result=joinMaps.map(x=> x._1+","+x._2._1._1._1+","+x._2._1._1._2+","+x._2._1._2+","+x._2._2+","+graphCache.numEdges+","+ graphCache.numVertices)
    val result=joinMaps.map(x=> x._1+","+x._2._1._1+","+x._2._1._2+","+x._2._2+","+numNodes+","+numEdges)
    
     
    result.saveAsTextFile(outputfile)
    
   
    //mapNodeDegre.collect().foreach(println(_))

    //mapNodeDegre.map(x=>x._1+","+x._2._1+","+x._2._2).saveAsTextFile(outputFile)
    /*val idg = ComputeDegree.inDegree(graph)
    idg.vertices.collect.foreach{println(_)}

    val odg = ComputeDegree.outDegree(graph)
    odg.vertices.collect.foreach{println(_)}

    val pr = graph.staticPageRank(20)
    pr.vertices.collect.foreach{println(_)}
    val b = ComputeBetweenness.betweenness(graph)
    b.vertices.collect.foreach{println(_)}*/

  }

}
