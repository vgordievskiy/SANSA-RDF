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
object ShortestPaths2 {
  /** Stores a map from the vertex id of a landmark to the distance to that landmark. */
  type SPMap = Map[VertexId, Set[XNestedPath[VertexId, String]]]

  private def makeMap(x: (VertexId, Set[XNestedPath[VertexId, String]])*) = Map(x: _*)

  /*
  private def makeMap(x: (VertexId, Int)*) = Map(x: _*)

  private def incrementMap(spmap: SPMap): SPMap = spmap.map { case (v, d) => v -> (d + 1) }

*/
  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> {
        val x = spmap1.getOrElse(k, Set());
        val y = spmap2.getOrElse(k, Set())
        x ++ y
      }
    }.toMap

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
  def run[VD](graph: Graph[VD, String], landmarks: Seq[VertexId]): Graph[SPMap, String] = {
    val spGraph = graph.mapVertices { (vid, attr) =>
      if (landmarks.contains(vid)) makeMap(vid -> Set(XNestedPath(None, vid))) else makeMap()
    }

    val initialMessage = makeMap()

    /**
     * Returns the new list of shortest paths
     */
    def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
      //println("VERTEX PROGRAM AT " + id)
      addMaps(attr, msg)
    }

    def sendMessage(edge: EdgeTriplet[SPMap, String]): Iterator[(VertexId, SPMap)] = {
//      val newAttr = incrementMap(edge.dstAttr)
//      if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
//      else Iterator.empty
      //val x = edge.attr

      //edge.dstAttr
      //println("MSG: " + edge)

      val tmp = edge.srcAttr.map({case (k, v) => k -> v.map(x => XNestedPath[VertexId, String](Some(XParentLink(x, XDirectedProperty(edge.attr, false))), edge.dstId))  })

      val combined = addMaps(edge.dstAttr, tmp)

      if (edge.dstAttr != combined)
        Iterator((edge.dstId, combined))
      else
        Iterator.empty
    }

    PregelShortestPaths(spGraph, initialMessage)(vertexProgram, sendMessage, addMaps)
  }
}