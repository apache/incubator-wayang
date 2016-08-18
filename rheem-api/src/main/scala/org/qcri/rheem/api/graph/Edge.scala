package org.qcri.rheem.api.graph

/**
  * Helper to handle [[Edge]]s.
  */
object Edge {

  /**
    * Create a new edge.
    *
    * @param src  the source [[Vertex]]
    * @param dest the target [[Vertex]]
    * @return the [[Edge]]
    */
  def apply(src: Long, dest: Long) = new Edge(src, dest)

}
