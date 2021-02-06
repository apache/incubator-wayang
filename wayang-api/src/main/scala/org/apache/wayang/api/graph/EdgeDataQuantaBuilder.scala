/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.api.graph

import org.apache.wayang.api.util.DataQuantaBuilderDecorator
import org.apache.wayang.api.{BasicDataQuantaBuilder, DataQuanta, DataQuantaBuilder, JavaPlanBuilder, _}
import org.apache.wayang.basic.operators.PageRankOperator
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval

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
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.MapOperator]]s.
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
