package org.dissect.rdf.spark.analytics

case class NestedPath[V, E](val parent : NestedPath[V, E], val currentNode : V, val diProperty : DirectedProperty[E]);
