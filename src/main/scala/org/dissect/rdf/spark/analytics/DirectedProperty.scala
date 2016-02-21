package org.dissect.rdf.spark.analytics

case class XDirectedProperty[E](val property : E, val isReverse : Boolean = false)