package org.dissect.rdf.spark.analytics

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Iterator

/**
 * Computes shortest paths to the given set of landmark vertices, returning a graph where each
 * vertex attribute is a map containing the shortest-path distance to each reachable landmark.
 */
object ShortestPaths3 {
  /** Stores a map from the vertex id of a landmark to the distance to that landmark. */
  type Frontier = Set[XNestedPath[VertexId, String]]

  //private def makeMap(x: (VertexId, Set[NestedPath[VertexId, String]])*) = Map(x: _*)

  /*
  private def makeMap(x: (VertexId, Int)*) = Map(x: _*)

  private def incrementMap(spmap: SPMap): SPMap = spmap.map { case (v, d) => v -> (d + 1) }

*/
//  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
//    (spmap1.keySet ++ spmap2.keySet).map {
//      k => k -> {
//        val x = spmap1.getOrElse(k, Set());
//        val y = spmap2.getOrElse(k, Set())
//        x ++ y
//      }
//    }.toMap

  private def addFrontiers(a: Frontier, b: Frontier): Frontier = a ++ b

  /**
   * Computes shortest paths to the given set of landmark vertices.
   *
   * @tparam ED the edge attribute type (not used in the computation)
   *
   * @param graph the graph for which to compute the shortest paths
   * @param landmarks the list of landmark vertex ids. Shortest paths will be computed to each
   * landmark.
   *
   * @return a graph where each vertex attribute is a map containing the shortest-path distance to
   * each reachable landmark vertex.
   */
  def run[VD](graph: Graph[VD, String], landmarks: Seq[VertexId]): Graph[Frontier, String] = {

    val spGraph : Graph[Frontier, String] = graph.mapVertices { (vid, frontier) =>
      if (landmarks.contains(vid)) Set(XNestedPath[VertexId, String](None, vid)) else Set()
    }

    val initialMessage: Frontier = Set[XNestedPath[VertexId, String]]()

    def vertexProgram(id: VertexId, attr: Frontier, msg: Frontier): Frontier = {
      addFrontiers(attr, msg)
    }

    def sendMessage(edge: EdgeTriplet[Frontier, String]): Iterator[(VertexId, Frontier)] = {
      val oldFrontier = edge.srcAttr
      var newFrontier = oldFrontier.map(item => XNestedPath[VertexId, String](Some(XParentLink(item, XDirectedProperty(edge.attr, false))), edge.dstId))

      newFrontier = newFrontier.filter(p => p.asSimplePath().isCycleFree())

      if (edge.dstAttr != newFrontier)
        Iterator((edge.dstId, newFrontier))
      else
        Iterator.empty
    }

    val foo = PregelShortestPaths(spGraph, initialMessage, Int.MaxValue, EdgeDirection.Out)_ // (vertexProgram, sendMessage, addSets)

    val x = foo(vertexProgram, sendMessage, addFrontiers)
    x
  }
}