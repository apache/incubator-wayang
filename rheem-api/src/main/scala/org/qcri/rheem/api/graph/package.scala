package org.qcri.rheem.api

import org.qcri.rheem.basic.data.{Tuple2 => T2}

/**
  * Provides implicits for Rheem's graph API.
  */
package object graph {

  implicit def elevateEdgeDataQuanta(dataQuanta: DataQuanta[T2[Integer, Integer]]): EdgeDataQuanta =
    new EdgeDataQuanta(dataQuanta)

}
