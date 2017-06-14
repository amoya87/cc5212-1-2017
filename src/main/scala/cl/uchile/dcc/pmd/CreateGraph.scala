package cl.uchile.dcc.pmd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.SparkSession
import com.centrality.kBC.KBetweenness
import org.apache.spark.graphx.lib.PageRank



object CreateGraph {
  
  def main(args: Array[String]) {
    
    System.out.println(args.length)

    if (args.length != 4) {
      println("Usage: [jsonInputfile] [outputfileEdges] [outputFileVertices] [resultFile]")
      //exit(1)
    }
    val master = "local[*]";
    val inputFile = args(0)
    val outputEdgesFile = args(1)
    val outputNodesFile = args(2)
    val resultFile = args(3)
    val conf = new SparkConf().setAppName("CreateGraph").setMaster(master)
    val sc = new SparkContext(conf)

    val spark =  SparkSession
      .builder()
      .appName("SomeAplication")
      .config("spark.master", "local")
      .getOrCreate();

    val jsonMap = sc.wholeTextFiles(inputFile).map(x => x._2)
    val jsonRdd = spark.read.json(jsonMap)

    jsonRdd.createOrReplaceTempView("network")
    jsonRdd.printSchema()

    val dfarcs = jsonRdd.select(jsonRdd.col("arcs"))
    val arcsdf=dfarcs.select(org.apache.spark.sql.functions.explode(jsonRdd.col("arcs"))).toDF("arcs")
    val arcs = arcsdf.select("arcs.endId", "arcs.id", "arcs.lanes", "arcs.startId", "arcs.type")
    arcs.createOrReplaceTempView("arcs")


    val dfnodes = jsonRdd.select(jsonRdd.col("nodes"))
    val nodesdf=dfnodes.select(org.apache.spark.sql.functions.explode(jsonRdd.col("nodes"))).toDF("nodes")
    val nodes = nodesdf.select("nodes.id", "nodes.lat", "nodes.lon", "nodes.osm_id")
    nodes.createOrReplaceTempView("nodes")


    val filterArcs = spark.sql("SELECT startId, endId FROM arcs")
    val filterNodes = spark.sql("SELECT id, lat, lon FROM nodes")
    val arcsRdd=filterArcs.rdd
    val arcsCache=arcsRdd.cache()
    val nodesRdd=filterNodes.rdd
    val nodesCache=nodesRdd.cache()

    //val relationships:RDD[Edge[VertexId]]=arcsCache.map(x=>new Edge(x.get(0).asInstanceOf[Number].longValue, x.get(1).asInstanceOf[Number].longValue, 0L))
    //relationships.collect().foreach(println(_))

    //val defaultUser = -1L
    // Build the initial Graph
    //val graph = Graph.fromEdges(relationships, defaultUser)
    
    val nodesMap= nodesRdd.map(x=>x.get(0).asInstanceOf[Number].longValue+","+x.get(1)+","+ x.get(2))
    val arcsMap= arcsRdd.map(x=>x.get(0)+","+x.get(1))
    
    nodesMap.saveAsTextFile(outputNodesFile)
    arcsMap.saveAsTextFile(outputEdgesFile)    
    
    
     val rawDataEdges = sc.textFile(outputEdgesFile)
    val rawDataNodes = sc.textFile(outputNodesFile)
    
    def convertToEdges(line: String)={
      val txt=line.split(",")
      new Edge(txt(0).toLong, txt(1).toLong, 0L)
    }  
     
    val edgesRDD:RDD[Edge[VertexId]] = rawDataEdges.map(x=> convertToEdges(x))
    
    val nodesRDD: RDD[(VertexId,Any)]= rawDataNodes.map(x=> x.split(",")).map(y=> (y(0).toLong, y(1)+","+ y(2)))
        

    
    //val relationships:RDD[Edge[VertexId]]=arcsCache.map(x=>new Edge(x.get(0).asInstanceOf[Number].longValue, x.get(1).asInstanceOf[Number].longValue, 0L))
    
    
    val edgesC=edgesRDD.cache()
    
    edgesC.collect().foreach(println(_))    
    nodesRDD.collect().foreach(println(_)) 
    // Build the initial Graph
    
    // Define a default user in case there are relationship with missing user
    val defaultUser = -1L
    
    val graph = Graph(nodesRDD, edgesRDD, defaultUser)
    
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
    
    System.out.println("PageRank------>")
    val rankGraph=PageRank.run(graphCache,10, 0.15)    
    val rankMap= rankGraph.vertices.map(x=>(x._1.toLong,x._2.toString))    
    //rankMap.collect().foreach(println(_))
    
    System.out.println("Nodos------>")
    val nodesOut= nodesRDD.map(x=> (x._1,x._2))
    //val joinMaps=nodesMap.join(dgMapCache).join(closeMapCache).join(kbMapCache)
    val joinMaps=nodesOut.join(dgMapCache).join(kbMapCache).join(rankMap)
    //val result=joinMaps.map(x=> x._1+","+x._2._1._1._1+","+x._2._1._1._2+","+x._2._1._2+","+x._2._2+","+graphCache.numEdges+","+ graphCache.numVertices)
    val result=joinMaps.map(x=> x._1+","+x._2._1._1._1+","+x._2._1._1._2+","+x._2._1._2+","+x._2._2+","+numNodes+","+numEdges)
    
     
    result.saveAsTextFile(resultFile)
    
    

  }
}