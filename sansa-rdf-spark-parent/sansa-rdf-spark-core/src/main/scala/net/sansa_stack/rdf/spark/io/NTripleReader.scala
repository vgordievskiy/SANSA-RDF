package net.sansa_stack.rdf.spark.io

import java.io.ByteArrayInputStream
import java.net.URI

import org.apache.jena.atlas.iterator.IteratorResourceClosing
import org.apache.jena.graph.Triple
import org.apache.jena.riot.lang.RiotParsers
import org.apache.jena.riot.system.ParserProfile
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * An N-Triples reader. One triple per line is assumed.
 *
 * @author Lorenz Buehmann
 */
object NTripleReader {

  /**
   * Loads an N-Triples file into an RDD.
   *
   * @param session the Spark session
   * @param path    the path to the N-Triples file(s)
   * @return the RDD of triples
   */
  def load(session: SparkSession, path: URI): RDD[Triple] = {
    load(session, path.toString)
  }

  /**
   * Loads an N-Triples file into an RDD.
   *
   * @param session the Spark session
   * @param path    the path to the N-Triples file(s)
   * @return the RDD of triples
   */
  def load(session: SparkSession, path: String): RDD[Triple] = {
    session.sparkContext.textFile(path)
      .filter(line => !line.trim().isEmpty & !line.startsWith("#"))
      .map(line =>
        RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(line.getBytes), Lang.NTRIPLES, null).next())
  }

  /**
    * Loads an N-Triples file into an RDD.
    *
    * @param session the Spark session
    * @param path    the path to the N-Triples file(s)
    * @param profile the parser profile
    * @return the RDD of triples
    */
  def load(session: SparkSession, path: String, profile: ParserProfile): RDD[Triple] = {
    session.sparkContext.textFile(path)
      .filter(line => !line.trim().isEmpty & !line.startsWith("#"))
      .map(line => {
        val is = new ByteArrayInputStream(line.getBytes)
        new IteratorResourceClosing[Triple](
          RiotParsers.createIteratorNTriples(is, null, profile), is).next()
      })
  }

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("spark session example")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.config("spark.kryo.registrationRequired", "true")
      //.config("spark.eventLog.enabled", "true")
      .config("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator"))
      .config("spark.default.parallelism", "4")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    val rdd = NTripleReader.load(sparkSession, URI.create(args(0)))

    println(rdd.take(10).mkString("\n"))
  }

}
