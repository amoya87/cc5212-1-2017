package cl.uchile.dcc.pmd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object App {
  
  def main(args: Array[String]) {

    if (args.length < 2) {
      println("Usage: [jsonInputfile] [outputfile]")
      //exit(1)
    }
    val master = "local[*]";
    val inputFile = args(0)
    val outputFile = args(1)
    val conf = new SparkConf().setAppName("SomeAppName").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val spark =  SparkSession
      .builder()
      .appName("SomeAppName")
      .config("spark.master", "local")
      .getOrCreate();

    val jsonMap = sc.wholeTextFiles(inputFile).map(x => x._2)

   // val jsonMap=jsonFile.map(x => x._2)
    val jsonRdd = spark.read.json(jsonMap)

    //jsonRdd.collect().foreach(println(_))

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

    val relationships:RDD[Edge[VertexId]]=arcsCache.map(x=>new Edge(x.get(0).asInstanceOf[Number].longValue, x.get(1).asInstanceOf[Number].longValue, 0L))
    relationships.collect().foreach(println(_))

    /*
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
      Edge(7L, 8L, 0L)))*/
    // Define a default user in case there are relationship with missing user
    val defaultUser = -1L
    // Build the initial Graph
    val graph = Graph.fromEdges(relationships, defaultUser)

    val shortestPaths = MyShortestPaths.run(graph)

    shortestPaths.vertices.collectAsMap().foreach(println(_))

   /* val c = ComputeCloseness.closeness(graph)
    c.triplets.saveAsTextFile(outputFile)*/

    val dg = ComputeDegree.degree(graph)

    val dgMap= dg.vertices.map(x=>(x._1.toLong,x._2.toString))
    val nodesMap= nodesRdd.map(x=>(x.get(0).asInstanceOf[Number].longValue,x.get(1)+","+ x.get(2)))
    nodesMap.collect().foreach(println(_))
    val mapNodeDegre=nodesMap.join(dgMap)

    //mapNodeDegre.collect().foreach(println(_))

    mapNodeDegre.map(x=>x._1+","+x._2._1+","+x._2._2).saveAsTextFile(outputFile)
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
