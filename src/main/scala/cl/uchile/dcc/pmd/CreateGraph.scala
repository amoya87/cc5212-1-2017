package cl.uchile.dcc.pmd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.SparkSession



object CreateGraph {
  
  def main(args: Array[String]) {
    
    System.out.println(args.length)

    if (args.length < 2) {
      println("Usage: [jsonInputfile] [outputfileEdges] [outputFileVertices]")
      //exit(1)
    }
    val master = "local[*]";
    val inputFile = args(0)
    val outputEdgesFile = args(1)
    val outputNodesFile = args(2)
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

  }
}