package org.dissect.rdf.spark.analytics

case class DirectedProperty[E](val property : E, val isReverse : Boolean = false)