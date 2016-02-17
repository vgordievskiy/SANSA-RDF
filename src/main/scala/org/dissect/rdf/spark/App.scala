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

object App extends Logging {


  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: FileName <path-to-files> <output-path>")
      System.exit(1)
    }

    //val i0 = null.asInstanceOf[NestedPath[Int, String]]

    // ParentLink(NestedPath parent, DiProperty)
    val i1 = NestedPath[Int, String](None, 1)
    val i2 = NestedPath(Some(ParentLink(i1, DirectedProperty("bar"))), 2)
    val nested = NestedPath(Some(ParentLink(i2, DirectedProperty("baz"))), 3)

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

    doWork(graphLayout.graph, graphLayout.iriToId)
    sparkContext.stop()
  }

  def doWork(graph : Graph[String, String], iriToId : RDD[(String, VertexId)]) = {
    //val graph = graphLayout.graph
    val vertexIds = iriToId.lookup("http://fp7-pp.publicdata.eu/resource/funding/223894-999854564")

    val roundtrip = graph.vertices.lookup(vertexIds(0));

    println("VERTEX ID = " + vertexIds.mkString("."))
    print("Roundtrip iri: " + roundtrip.mkString("."))
//
    val landmarks = Seq[Long](1, 2, 3)
    val result = ShortestPaths2.run(graph, landmarks)

    //val foo = result.mapVertices(x => )

    println("RESULT")
    result.vertices.foreach({ case(vertexId, map) => {
      map.values.flatMap(identity).foreach(p => {
        println("Path" + p.asSimplePath()) //.mapV { v => graph.vertices.lookup(v) })
      })
//        {case (v, d) => {
//      println("Vertex: " + v)
//      d.foreach(p => {
//        p.
//      })
//      println(x)
    }})




    logger.info("RDFModel..........executed")


    logger.info("Graph stats: " + graph.numVertices + " - " + graph.numEdges)
  }
}

