package org.dissect.rdf.spark.analytics

case class RdfPath[V](startVertex : V, endVertex : V, triples : List[Triple]) {
  /**
   * The path is cycle free if no edge appears twice, that means that the list of
   * triples does not contain duplicates
   */
  def isCycleFree(): Boolean = {
    val n = triples.size
    val m = triples.toSet.size
    n == m
  }
}
