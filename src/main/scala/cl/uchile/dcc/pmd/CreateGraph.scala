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
import shapeless.ops.nat.ToInt
import org.apache.spark.graphx.PartitionStrategy



object CreateGraph {
  
  def main(args: Array[String]) {
    
    System.out.println(args.length)

    if (args.length != 6) {
      println("Usage: [jsonInputfile] [outputfileEdges] [outputFileVertices] [resultFile] [operation = *3|CreateGraph 1|CalcMetrics 2]")
    }
    val master = "local[*]";
    val inputFile = args(0)
    val outputEdgesFile = args(1)
    val outputNodesFile = args(2)
    val resultFile = args(3)
    val conf = new SparkConf().setAppName("CreateGraph").setMaster(master)
    val sc = new SparkContext(conf)
    val option = args(4)
    val operacion = args(5)
    
    if (option.toInt == 1) {      
    
    val spark =  SparkSession
      .builder()
      .appName("SomeAplication")
      .getOrCreate();

    val jsonMap = sc.wholeTextFiles(inputFile).map(x => x._2)
    val jsonRdd = spark.read.json(jsonMap)

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

    val nodesMap= nodesRdd.map(x=>x.get(0).asInstanceOf[Number].longValue+","+x.get(1)+","+ x.get(2))
    val arcsMap= arcsRdd.map(x=>x.get(0)+","+x.get(1))
    
    nodesMap.saveAsTextFile(outputNodesFile)
    arcsMap.saveAsTextFile(outputEdgesFile)    
  }
    
    if (option.toInt == 2) {
      
    
    val rawDataEdges = sc.textFile(outputEdgesFile)
    val rawDataNodes = sc.textFile(outputNodesFile)
    
    def convertToEdges(line: String)={
      val txt=line.split(",")
      new Edge(txt(0).toLong, txt(1).toLong, 0L)
    }  
     
    val edgesRDD:RDD[Edge[VertexId]] = rawDataEdges.map(x=> convertToEdges(x))
    
    val nodesRDD: RDD[(VertexId,Any)]= rawDataNodes.map(x=> x.split(",")).map(y=> (y(0).toLong, y(1)+","+ y(2)))
        
   
    val edgesC=edgesRDD.cache()
    
     val defaultUser = -1L
    
    val graph = Graph(nodesRDD, edgesRDD, defaultUser)
    
    val graphC=graph.cache();
    
    val numNodes=graph.numVertices
    val numEdges=graph.numEdges
 
    val nodesOut= nodesRDD.map(x=> (x._1,x._2))
    var result:RDD[String]=null
    if(operacion=="d"){
       val dg = ComputeDegree.degree(graphC)   
       val dgMap= dg.vertices.map(x=>(x._1.toLong,x._2.toString))
       val joinMaps=nodesOut.join(dgMap)       
       result=joinMaps.map(x=> x._1+","+x._2._1+","+x._2._2+","+numNodes+","+numEdges)
    }
    
    if(operacion=="b"){
       val kb = KBetweenness.run(graphC,3)   
       val kbMap= kb.vertices.map(x=>(x._1.toLong,x._2.toString))    
       val joinMaps=nodesOut.join(kbMap)
       result=joinMaps.map(x=> x._1+","+x._2._1+","+x._2._2+","+numNodes+","+numEdges)
    }
    
    if(operacion=="r"){
       val rankGraph=PageRank.run(graphC,10, 0.15)    
       val rankSum = rankGraph.vertices.values.sum()
       val graphRank=rankGraph.mapVertices((id, rank) => rank / rankSum)
       val rankMap= graphRank.vertices.map(x=>(x._1.toLong,x._2.toString))    
       val joinMaps=nodesOut.join(rankMap)
       result=joinMaps.map(x=> x._1+","+x._2._1+","+x._2._2+","+numNodes+","+numEdges)
    }        
    result.saveAsTextFile(resultFile)    
    }

  }
   
}
