package org.qcri.rheem.api.graph

import org.qcri.rheem.api.util.DataQuantaBuilderDecorator
import org.qcri.rheem.api.{BasicDataQuantaBuilder, DataQuanta, DataQuantaBuilder, JavaPlanBuilder, _}
import org.qcri.rheem.basic.operators.PageRankOperator
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval

/**
  * Enriches [[DataQuantaBuilder]] by graph-specific operations.
  */
trait EdgeDataQuantaBuilder[+This <: EdgeDataQuantaBuilder[This]]
  extends DataQuantaBuilder[This, Edge] {

  /**
    * Feed built [[DataQuanta]] into a [[PageRankOperator]].
    *
    * @param numIterations number of PageRank iterations to do
    * @return a new instance representing the [[PageRankOperator]]'s output
    */
  def pageRank(numIterations: Int): PageRankDataQuantaBuilder = new PageRankDataQuantaBuilder(this, numIterations)

}

/**
  * This decorator enriches a regular [[DataQuantaBuilder]] with operations of a [[RecordDataQuantaBuilder]].
  *
  * @param baseBuilder the [[DataQuantaBuilder]] to be enriched
  */
class EdgeDataQuantaBuilderDecorator[This <: EdgeDataQuantaBuilderDecorator[This]]
(baseBuilder: DataQuantaBuilder[_, Edge])
  extends DataQuantaBuilderDecorator[This, Edge](baseBuilder) with EdgeDataQuantaBuilder[This]

/**
  * [[DataQuantaBuilder]] implementation for [[org.qcri.rheem.basic.operators.MapOperator]]s.
  *
  * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
  * @param numIterations   number of PageRank iterations to do
  */
class PageRankDataQuantaBuilder(inputDataQuanta: DataQuantaBuilder[_, Edge],
                                numIterations: Int)
                               (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[PageRankDataQuantaBuilder, PageRank] {

  /** Presumed graph density. */
  private var graphDensity = PageRankOperator.DEFAULT_GRAPH_DENSITIY

  private var dampingFactor = PageRankOperator.DEFAULT_DAMPING_FACTOR

  // We statically know input and output data types.
  locally {
    inputDataQuanta.outputTypeTrap.dataSetType = dataSetType[Edge]
    this.outputTypeTrap.dataSetType = dataSetType[PageRank]
  }

  /**
    * Set the damping factor for the PageRank.
    *
    * @param dampingFactor the damping factor
    * @return this instance
    */
  def withDampingFactor(dampingFactor: Double) = {
    this.dampingFactor = dampingFactor
    this
  }

  /**
    * Set the graph density of the processed graph.
    *
    * @param graphDensity the graph density
    * @return this instance
    */
  def withGraphDensity(graphDensity: ProbabilisticDoubleInterval) = {
    this.graphDensity = graphDensity
    this
  }

  override protected def build = inputDataQuanta.dataQuanta().pageRank(numIterations, this.dampingFactor, this.graphDensity)

}