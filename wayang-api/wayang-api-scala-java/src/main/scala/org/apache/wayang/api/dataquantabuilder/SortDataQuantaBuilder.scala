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

package org.apache.wayang.api.dataquantabuilder

import org.apache.wayang.api.util.TypeTrap
import org.apache.wayang.api.{DataQuanta, DataQuantaBuilder, JavaPlanBuilder}
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction
import org.apache.wayang.core.optimizer.costs.LoadEstimator
import org.apache.wayang.core.types.DataSetType
import org.apache.wayang.core.util.ReflectionUtils

import scala.reflect.ClassTag

/**
 * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.SortOperator]]s.
 *
 * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
 * @param keyUdf          UDF for the [[org.apache.wayang.basic.operators.SortOperator]]
 */
class SortDataQuantaBuilder[T, Key](inputDataQuanta: DataQuantaBuilder[_, T],
                                    keyUdf: SerializableFunction[T, Key])
                                   (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[SortDataQuantaBuilder[T, Key], T] {

  // Reuse the input TypeTrap to enforce type equality between input and output.
  override def getOutputTypeTrap: TypeTrap = inputDataQuanta.outputTypeTrap

  /** [[ClassTag]] or surrogate of [[Key]] */
  implicit var keyTag: ClassTag[Key] = _

  /** [[LoadEstimator]] to estimate the CPU load of the [[keyUdf]]. */
  private var keyUdfCpuEstimator: LoadEstimator = _

  /** [[LoadEstimator]] to estimate the RAM load of the [[keyUdf]]. */
  private var keyUdfRamEstimator: LoadEstimator = _


  // Try to infer the type classes from the UDFs.
  locally {
    val parameters = ReflectionUtils.getTypeParameters(keyUdf.getClass, classOf[SerializableFunction[_, _]])
    parameters.get("Input") match {
      case cls: Class[T] => inputDataQuanta.outputTypeTrap.dataSetType = DataSetType.createDefault(cls)
      case _ => logger.warn("Could not infer types from {}.", keyUdf)
    }

    this.keyTag = parameters.get("Output") match {
      case cls: Class[Key] => ClassTag(cls)
      case _ =>
        logger.warn("Could not infer types from {}.", keyUdf)
        ClassTag(DataSetType.none.getDataUnitType.getTypeClass)
    }
  }


  /**
   * Set a [[LoadEstimator]] for the CPU load of the first key extraction UDF. Currently effectless.
   *
   * @param udfCpuEstimator the [[LoadEstimator]]
   * @return this instance
   */
  def withThisKeyUdfCpuEstimator(udfCpuEstimator: LoadEstimator) = {
    this.keyUdfCpuEstimator = udfCpuEstimator
    this
  }

  /**
   * Set a [[LoadEstimator]] for the RAM load of first the key extraction UDF. Currently effectless.
   *
   * @param udfRamEstimator the [[LoadEstimator]]
   * @return this instance
   */
  def withThisKeyUdfRamEstimator(udfRamEstimator: LoadEstimator) = {
    this.keyUdfRamEstimator = udfRamEstimator
    this
  }

  override protected def build =
    inputDataQuanta.dataQuanta().sortJava(keyUdf)(this.keyTag)

}
