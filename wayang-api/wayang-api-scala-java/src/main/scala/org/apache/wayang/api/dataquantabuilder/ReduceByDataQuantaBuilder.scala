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
import org.apache.wayang.core.function.FunctionDescriptor.{SerializableBinaryOperator, SerializableFunction}
import org.apache.wayang.core.optimizer.costs.{LoadProfile, LoadProfileEstimator}
import org.apache.wayang.core.types.DataSetType
import org.apache.wayang.core.util.ReflectionUtils

import scala.reflect.ClassTag

/**
 * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.ReduceByOperator]]s.
 *
 * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
 * @param udf             UDF for the [[org.apache.wayang.basic.operators.ReduceByOperator]]
 * @param keyUdf          key extraction UDF for the [[org.apache.wayang.basic.operators.ReduceByOperator]]
 */
class ReduceByDataQuantaBuilder[Key, T](inputDataQuanta: DataQuantaBuilder[_, T],
                                        keyUdf: SerializableFunction[T, Key],
                                        udf: SerializableBinaryOperator[T])
                                       (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[ReduceByDataQuantaBuilder[Key, T], T] {

  // Reuse the input TypeTrap to enforce type equality between input and output.
  override def getOutputTypeTrap: TypeTrap = inputDataQuanta.outputTypeTrap

  implicit var keyTag: ClassTag[Key] = _

  /** [[LoadProfileEstimator]] to estimate the [[LoadProfile]] of the [[udf]]. */
  private var udfLoadProfileEstimator: LoadProfileEstimator = _

  // TODO: Add these estimators.
  //  /** [[LoadEstimator]] to estimate the CPU load of the [[keyUdf]]. */
  //  private var keyUdfCpuEstimator: LoadEstimator = _
  //
  //  /** [[LoadEstimator]] to estimate the RAM load of the [[keyUdf]]. */
  //  private var keyUdfRamEstimator: LoadEstimator = _

  // Try to infer the type classes from the UDFs.
  locally {
    var parameters = ReflectionUtils.getTypeParameters(udf.getClass, classOf[SerializableBinaryOperator[_]])
    parameters.get("Type") match {
      case cls: Class[T] => this.outputTypeTrap.dataSetType = DataSetType.createDefault(cls)
      case _ => logger.warn("Could not infer types from {}.", udf)
    }

    parameters = ReflectionUtils.getTypeParameters(keyUdf.getClass, classOf[SerializableFunction[_, _]])
    parameters.get("Input") match {
      case cls: Class[T] => this.outputTypeTrap.dataSetType = DataSetType.createDefault(cls)
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
   * Set a [[LoadProfileEstimator]] for the load of the UDF.
   *
   * @param udfLoadProfileEstimator the [[LoadProfileEstimator]]
   * @return this instance
   */
  def withUdfLoad(udfLoadProfileEstimator: LoadProfileEstimator) = {
    this.udfLoadProfileEstimator = udfLoadProfileEstimator
    this
  }

  override protected def build =
    inputDataQuanta.dataQuanta().reduceByKeyJava(keyUdf, udf, this.udfLoadProfileEstimator)

}
