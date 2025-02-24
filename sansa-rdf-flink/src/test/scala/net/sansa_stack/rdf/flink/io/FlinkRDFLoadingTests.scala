package net.sansa_stack.rdf.flink.io

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

class FlinkRDFLoadingTests extends FunSuite {

  val env = ExecutionEnvironment.getExecutionEnvironment

  test("loading N-Triples file into DataSet should match") {

    val path = getClass.getResource("/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val cnt = triples.count()

    assert(cnt == 106)
  }

  test("loading N-Quads file into DataSet should match") {

    val path = getClass.getResource("/data.nq").getPath
    val lang: Lang = Lang.NQUADS

    val triples = env.rdf(lang)(path)

    val cnt = triples.count()

    assert(cnt == 28)
  }

  test("loading RDF/XML file into DataSet should match") {
    val path = getClass.getResource("/data.rdf").getPath

    val lang: Lang = Lang.RDFXML
    val triples = env.rdf(lang)(path)

    val cnt = triples.count()

    assert(cnt == 9)
  }

}
