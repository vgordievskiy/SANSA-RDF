package org.dissect.rdf.spark.analytics

case class RdfPath[V, E](startVertex : V, endVertex : V, triples : List[Triple[V, E]]) {
  /**
   * The path is cycle free if no edge appears twice, that means that the list of
   * triples does not contain duplicates
   */
  def isCycleFree(): Boolean = {
    val n = triples.size
    val m = triples.toSet.size
    n == m
  }

  def mapV[T](fn: (V) => T) : RdfPath[T, E] =
    RdfPath(fn(startVertex), fn(endVertex), triples.map(t => t.mapV(fn)))

  def mapE[T](fn: (E) => T) : RdfPath[V, T] =
    RdfPath(startVertex, endVertex, triples.map(t => t.mapE(fn)))
}
