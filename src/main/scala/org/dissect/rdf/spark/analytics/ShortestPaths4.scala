package org.dissect.rdf.spark.analytics

import java.util.Optional

import scala.Iterator
import scala.reflect.ClassTag

import org.aksw.jena_sparql_api_sparql_path2.DirectedProperty
import org.aksw.jena_sparql_api_sparql_path2.NestedPath
import org.aksw.jena_sparql_api_sparql_path2.ParentLink
import org.apache.spark.graphx._

/**
 * Computes shortest paths to the given set of landmark vertices, returning a graph where each
 * vertex attribute is a map containing the shortest-path distance to each reachable landmark.
 */
object ShortestPaths4 {
  /** Stores a map from the vertex id of a landmark to the distance to that landmark. */
  type Frontier[V, E] = Set[NestedPath[V, E]]

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

  private def addFrontiers[V, E](a: Frontier[V, E], b: Frontier[V, E]): Frontier[V, E] = a ++ b

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
  def run[V, E : ClassTag](graph: Graph[V, E], landmarks: Seq[V]): Graph[(V, Frontier[V, E]), E] = {

    val spGraph : Graph[(V, Frontier[V, E]), E] = graph.mapVertices { (vid, attr) =>
      attr -> (if (landmarks.contains(attr)) Set(new NestedPath[V, E](attr)) else Set())
    }

    val initialMessage: Frontier[V, E] = Set[NestedPath[V, E]]()

    def vertexProgram(id: VertexId, attr: (V, Frontier[V, E]), msg: Frontier[V, E]): (V, Frontier[V, E]) = {
      attr._1 -> addFrontiers(attr._2, msg)
    }

    def sendMessage(edge: EdgeTriplet[(V, Frontier[V, E]), E]): Iterator[(VertexId, Frontier[V, E])] = {
      val oldFrontier = edge.srcAttr._2
      var newFrontier = oldFrontier.map(item => new NestedPath[V, E](Optional.of(new ParentLink[V, E](item, new DirectedProperty[E](edge.attr, false))), edge.dstAttr._1))

      newFrontier = newFrontier.filter(_.asSimplePath().isCycleFree())

      if (edge.dstAttr != newFrontier)
        Iterator((edge.dstId, newFrontier))
      else
        Iterator.empty
    }
//(V, Frontier[V, E]), E, Frontier[V, E]
    val foo = PregelShortestPaths(spGraph, initialMessage, Int.MaxValue, EdgeDirection.Out)_ // (vertexProgram, sendMessage, addSets)

    val x = foo(vertexProgram, sendMessage, addFrontiers)
    x
  }
}