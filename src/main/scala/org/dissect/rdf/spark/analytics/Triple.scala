package org.dissect.rdf.spark.analytics

/**
 * This kind of triple is more generic than that of RDF
 */
object Triple {
  def swap[V, E](t : Triple[V, E]) : Triple[V, E] = {
    new Triple[V, E](t.o, t.p, t.s)
  }
}

case class Triple[V, E](val s : V, val p : E, val o : V) {
  def mapV[X](fn: (V) => X) : Triple[X, E] = {
    Triple(fn(s), p, fn(o))
  }

  def mapE[X](fn: (E) => X) : Triple[V, X] = {
    Triple(s, fn(p), o)
  }
}