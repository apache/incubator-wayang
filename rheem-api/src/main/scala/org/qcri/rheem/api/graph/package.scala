package org.qcri.rheem.api

import org.qcri.rheem.basic.data.{Tuple2 => T2}

/**
  * Provides implicits for Rheem's graph API.
  */
package object graph {

  type Vertex = java.lang.Long

  type Edge = T2[Vertex, Vertex]

  type PageRank = T2[Vertex, java.lang.Float]

  implicit def elevateEdgeDataQuanta(dataQuanta: DataQuanta[Edge]): EdgeDataQuanta =
    new EdgeDataQuanta(dataQuanta)

}
