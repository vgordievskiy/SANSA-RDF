package org.dissect.rdf.spark.analytics

case class XParentLink[V, E](val target : XNestedPath[V, E], val diProperty : XDirectedProperty[E])

case class XNestedPath[V, E](val parentLink : Option[XParentLink[V, E]] = None, val currentNode : V) {

  def asSimplePath() : RdfPath[V, E] = {
    val end = currentNode
    var triples = List[Triple[V, E]]()
    var start = null.asInstanceOf[V]

    //var c : Option[XParentLink[V, E]] = Some(XParentLink[V, E](this, null.asInstanceOf[V]))
    // There most likely exists some beautiful solution to write this code in a scala idiomatic way. This is left as an exercise to the reader.
    var c = this
    while(c != null) {
      val o = c.currentNode

      c.parentLink match {
        case None => {
          start = o
          c = null
        }
        case Some(parentLink) => {
          val dp = parentLink.diProperty
          val p = dp.property
          val s = parentLink.target.currentNode
          val t = Triple(s, p, o)

          val i = if(dp.isReverse) t else Triple.swap(t)
          triples ::= t

          c = parentLink.target
        }
      }
    }

    RdfPath[V, E](start, end, triples)
  }

//  def mapV[T](fn: (V) => T) : RdfPath[T, E] =
//    RdfPath(fn(startVertex), fn(endVertex), triples.map(t => t.mapV(fn)))
//
//  def mapE[T](fn: (E) => T) : RdfPath[V, T] =
//    RdfPath(startVertex, endVertex, triples.map(t => t.mapE(fn)))


//     public boolean isCycleFree() {
//        boolean result = asSimplePath().isCycleFree();
//        return result;
//    }
//
//    public int getLength() {
//        int result = asSimplePath().getLength();
//        return result;
//    }
//
//    public RdfPath asSimplePath() {
//        Node end = current;
//
//        NestedRdfPath c = this;
//        Node start = end;
//        List<Triple> triples = new ArrayList<Triple>();
//        while(c != null) {
//            Node o = c.getCurrent();
//            NestedRdfPath pr = c.getParent();
//
//            if(pr == null) {
//                start = o;
//            } else {
//                Node p = c.getParentProperty();
//                Node s = pr.getCurrent();
//
//                Triple triple = new Triple(s, p, o);
//                if(c.isReverse()) {
//                    triple = TripleUtils.swap(triple);
//                }
//
//                triples.add(triple);
//            }
//            c = c.getParent();
//        }
//
//        Collections.reverse(triples);
//        RdfPath result = new RdfPath(start, end, triples);
//        return result;
//    }

}
