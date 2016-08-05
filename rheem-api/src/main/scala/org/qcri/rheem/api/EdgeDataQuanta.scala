package org.qcri.rheem.api

import org.qcri.rheem.basic.data.{Record, Tuple2 => T2}
import org.qcri.rheem.basic.operators.{MapOperator, PageRankOperator}

/**
  * This class enhances the functionality of [[DataQuanta]] with [[Record]]s.
  */
class EdgeDataQuanta(dataQuanta: DataQuanta[T2[Integer, Integer]]) {

  implicit def planBuilder: PlanBuilder = dataQuanta.planBuilder

  /**
    * Feed this instance into a [[PageRankOperator]].
    *
    * @param numIterations number of PageRank iterations
    * @return a new instance representing the [[MapOperator]]'s output
    */
  def pageRank(numIterations: Int = 20): DataQuanta[T2[Integer, Float]] = {
    val pageRankOperator = new PageRankOperator(numIterations)
    dataQuanta.connectTo(pageRankOperator, 0)
    wrap[T2[Integer, Float]](pageRankOperator)
  }

}
