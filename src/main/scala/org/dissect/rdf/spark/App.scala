package org.dissect.rdf.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source
import java.io.File
import org.apache.commons.io.FileUtils
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkConf
import org.apache.jena.riot.RiotReader
import org.apache.jena.riot.Lang
import org.dissect.rdf.spark.utils._
import org.dissect.rdf.spark.model._
import org.dissect.rdf.spark.analytics._
import org.dissect.rdf.spark.utils.Logging
import org.dissect.rdf.spark.graph.LoadGraph
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.jena.sparql.util.NodeUtils
import org.aksw.jena_sparql_api_sparql_path2.ParentLink
import org.aksw.jena_sparql_api_sparql_path2.NestedPath
import org.aksw.jena_sparql_api_sparql_path2.DirectedProperty
import java.util.Optional
import org.apache.spark.rdd.PairRDDFunctions
import scala.reflect.ClassTag

object App extends Logging {


  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: FileName <path-to-files> <output-path>")
      System.exit(1)
    }

    //val i0 = null.asInstanceOf[NestedPath[Int, String]]

    // ParentLink(NestedPath parent, DiProperty)
    val i1 = new NestedPath[Int, String](1) //.asInstanceOf[ParentLink[Int, String]]
    val i2 = new NestedPath(Optional.of(new ParentLink(i1, new DirectedProperty("bar"))), 2)
    val nested = new NestedPath(Optional.of(new ParentLink(i2, new DirectedProperty("baz"))), 3)

    val simple = nested.asSimplePath();
    println(nested)
    println(simple)

    //System.exit(0)

    val fileName = args(0)
    val sparkMasterHost = if(args.length >= 2) args(1) else SparkUtils.SPARK_MASTER

    val sparkConf = new SparkConf().setAppName("BDE-readRDF").setMaster(sparkMasterHost);
    val sparkContext = new SparkContext(sparkConf)

    //val file = "C:/Users/Gezimi/Desktop/AKSW/Spark/sparkworkspace/data/nyse.nt"
    val graphLayout = LoadGraph(fileName, sparkContext)

    val typedGraph: Graph[(String, Iterable[String]), String] = createTypedGraph(graphLayout.graph, x => "http://www.w3.org/1999/02/22-rdf-syntax-ns#type".equals(x))

    typedGraph.vertices.foreach { println _ }


    //val typedV = graph.joinVertices(srcVidToTypeUris)((a, b: String, c) => b)


    //doWork(typedGraph, graphLayout.iriToId)
    sparkContext.stop()
  }

  def createTypedGraph[VD : ClassTag, ED : ClassTag](graph: Graph[VD, ED], typePredicate : ED => Boolean) = {
    val rdfTypeRdd = graph.edges
      .filter { edge => typePredicate(edge.attr) }
      .map { x => x.dstId -> x.srcId }

    val srcVidToTypeUris = rdfTypeRdd
      .join(graph.vertices)
      .map(_._2)
      .groupByKey()

    val typedVertices: RDD[(VertexId, (VD, Iterable[VD]))] = graph.vertices.join(srcVidToTypeUris)

    val typedGraph: Graph[(VD, Iterable[VD]), ED] = Graph(typedVertices, graph.edges)
    typedGraph
  }

  def doWork(graph : Graph[String, String], iriToId : RDD[(String, VertexId)]) = {

    //val graph = graphLayout.graph
    //val vertexIds = iriToId.lookup("http://fp7-pp.publicdata.eu/resource/funding/223894-999854564")
    //val landmarks = iriToId.lookup("http://fp7-pp.publicdata.eu/resource/project/257943")
    val landmarks = Seq("http://fp7-pp.publicdata.eu/resource/project/257943")

//    val roundtrip = graph.vertices.lookup(landmarks(0));
//
//    println("VERTEX ID = " + landmarks.mkString("."))
//    print("Roundtrip iri: " + roundtrip.mkString("."))
//
    //val landmarks = Seq[Long](1, 2, 3)
    //val landmarks = Seq[Long](
    val result = ShortestPaths4.run(graph, landmarks)

    result.vertices.foreach {
      case(i, (v, frontier)) => frontier.foreach { path => println(path.asSimplePath()) }
    }

    //result.vertices.z

//    result.vertices.foreach({case(v, frontier) =>
//      frontier.foreach(path => print("Path: " + path.asSimplePath().mapV {
//        x =>
//          { println("X = " + x)
//            val r = graph.vertices.lookup(x)
//            r
//          }
//      }))
//    })

    //val foo = result.mapVertices(x => )
//    val mapped = result.mapVertices({ case(vertexId, map) => vertexId ->
//      map.values.flatMap(identity).map(p => {
//        p.asSimplePath().mapV { v => graph.vertices.lookup(v) }
//        //println("Path" + p.asSimplePath().mapV { v => graph.vertices.lookup(v) })
//      })
//    })

    //val human = result.vertices.cogroup(graph.vertices)
  //val human = result.vertices.leftOuterJoin(graph.vertices)
    //val human = result.vertices.zip(other)(graph.vertices)

    //human.foreach({ case(v, (frontier, l)) => frontier.foreach(path => print(v + "-" + path.asSimplePath.mapV { x =>  }())) })

    //mapped.vertices.foreach({case (k, v) => println(v)})

//    println("RESULT")
//    result.vertices.foreach({ case(vertexId, map) => {
//      map.values.flatMap(identity).foreach(p => {
//        println("Path" + p.asSimplePath()) //.mapV { v => graph.vertices.lookup(v) })
//      })
//        {case (v, d) => {
//      println("Vertex: " + v)
//      d.foreach(p => {
//        p.
//      })
//      println(x)




    logger.info("RDFModel..........executed")


    logger.info("Graph stats: " + graph.numVertices + " - " + graph.numEdges)
  }
}

