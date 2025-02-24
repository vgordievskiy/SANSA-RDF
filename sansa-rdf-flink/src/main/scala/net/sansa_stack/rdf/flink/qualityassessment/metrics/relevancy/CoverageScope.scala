package net.sansa_stack.rdf.flink.qualityassessment.metrics.relevancy

import net.sansa_stack.rdf.common.qualityassessment.utils.NodeUtils._
import org.apache.flink.api.scala._
import org.apache.jena.graph.{ Node, Triple }

/**
 * @author Gezim Sejdiu
 */
object CoverageScope {

  /**
   * This metric calculate the coverage of a dataset referring to the covered scope.
   * This covered scope is expressed as the number of 'instances' statements are made about.
   */
  def assessCoverageScope(dataset: DataSet[Triple]): Double = {

    val triples = dataset.count().toDouble

    // ?o a rdfs:Class UNION ?o a owl:Class
    val instances = dataset.filter(f => isRDFSClass(f.getPredicate)).map(_.getObject).distinct()
      .union(dataset.filter(f => isOWLClass(f.getPredicate)).map(_.getObject).distinct())
      .count().toDouble

    val value = if (triples > 0.0) {
      instances / triples
    } else 0

    value
  }
}
