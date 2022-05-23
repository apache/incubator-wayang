/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package org.apache.wayang.api.dataquanta.builder

import org.apache.wayang.api.JavaPlanBuilder
import org.apache.wayang.api.dataquanta.{DataQuanta, DataQuantaBuilder}
import org.apache.wayang.api.util.TypeTrap
import org.apache.wayang.basic.operators.SampleOperator

import java.util.function.IntUnaryOperator

/**
 * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.SampleOperator]]s.
 *
 * @param inputDataQuanta    [[DataQuantaBuilder]] for the input [[DataQuanta]]
 * @param sampleSizeFunction the absolute size of the sample as a function of the current iteration number
 */
class SampleDataQuantaBuilder[T](inputDataQuanta: DataQuantaBuilder[_, T], sampleSizeFunction: IntUnaryOperator)
                                (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[SampleDataQuantaBuilder[T], T] {

  /**
   * Size of the dataset to be sampled.
   */
  private var datasetSize = SampleOperator.UNKNOWN_DATASET_SIZE

  /**
   * Sampling method to use.
   */
  private var sampleMethod = SampleOperator.Methods.ANY

  /**
   * Seed to use.
   */
  private var seed: Option[Long] = None

  // Reuse the input TypeTrap to enforce type equality between input and output.
  override def getOutputTypeTrap: TypeTrap = inputDataQuanta.outputTypeTrap

  /**
   * Set the size of the dataset that should be sampled.
   *
   * @param datasetSize the size of the dataset
   * @return this instance
   */
  def withDatasetSize(datasetSize: Long) = {
    this.datasetSize = datasetSize
    this
  }

  /**
   * Set the sample method to be used.
   *
   * @param sampleMethod the sample method
   * @return this instance
   */
  def withSampleMethod(sampleMethod: SampleOperator.Methods) = {
    this.sampleMethod = sampleMethod
    this
  }

  /**
   * Set the sample method to be used.
   *
   * @param seed
   * @return this instance
   */
  def withSeed(seed: Long) = {
    this.seed = Some(seed)
    this
  }

  override protected def build =
    inputDataQuanta.dataQuanta().sampleDynamicJava(sampleSizeFunction, this.datasetSize, this.seed, this.sampleMethod)

}
