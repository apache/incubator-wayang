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
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval
import org.apache.wayang.core.optimizer.costs.{LoadProfile, LoadProfileEstimator}
import org.apache.wayang.core.types.DataSetType
import org.apache.wayang.core.util.ReflectionUtils

/**
 * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.MapPartitionsOperator]]s.
 *
 * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
 * @param udf             UDF for the [[org.apache.wayang.basic.operators.MapPartitionsOperator]]
 */
class MapPartitionsDataQuantaBuilder[In, Out](inputDataQuanta: DataQuantaBuilder[_, In],
                                              udf: SerializableFunction[java.lang.Iterable[In], java.lang.Iterable[Out]])
                                             (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[MapPartitionsDataQuantaBuilder[In, Out], Out] {

  /** [[LoadProfileEstimator]] to estimate the [[LoadProfile]] of the [[udf]]. */
  private var udfLoadProfileEstimator: LoadProfileEstimator = _

  /** Selectivity of the filter predicate. */
  private var selectivity: ProbabilisticDoubleInterval = _

  // Try to infer the type classes from the udf.
  locally {
    val parameters = ReflectionUtils.getTypeParameters(udf.getClass, classOf[SerializableFunction[_, _]])
    parameters.get("Input") match {
      case cls: Class[In] => {
        inputDataQuanta.outputTypeTrap.dataSetType = DataSetType.createDefault(cls)
      }
      case _ => logger.warn("Could not infer types from {}.", udf)
    }
    val originalClass = ReflectionUtils.getWrapperClass(parameters.get("Output"), 0)
    originalClass match {
      case cls: Class[Out] => {
        this.outputTypeTrap.dataSetType = DataSetType.createDefault(cls)
      }
      case _ => logger.warn("Could not infer types from {}.", udf)
    }
  }


  /**
   * Set a [[LoadProfileEstimator]] for the load of the UDF.
   *
   * @param udfLoadProfileEstimator the [[LoadProfileEstimator]]
   * @return this instance
   */
  def withUdfLoad(udfLoadProfileEstimator: LoadProfileEstimator) = {
    this.udfLoadProfileEstimator = udfLoadProfileEstimator
    this
  }

  /**
   * Specify the selectivity of the UDF.
   *
   * @param lowerEstimate the lower bound of the expected selectivity
   * @param upperEstimate the upper bound of the expected selectivity
   * @param confidence    the probability of the actual selectivity being within these bounds
   * @return this instance
   */
  def withSelectivity(lowerEstimate: Double, upperEstimate: Double, confidence: Double) = {
    this.selectivity = new ProbabilisticDoubleInterval(lowerEstimate, upperEstimate, confidence)
    this
  }

  override protected def build = inputDataQuanta.dataQuanta().mapPartitionsJava(
    udf, this.selectivity, this.udfLoadProfileEstimator
  )

}
