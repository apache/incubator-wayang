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

import org.apache.wayang.api.dataquanta.{DataQuanta, DataQuantaBuilder}
import org.apache.wayang.api.{JavaPlanBuilder, dataSetType}
import org.apache.wayang.basic.data.{Tuple2 => WT2}
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction
import org.apache.wayang.core.optimizer.costs.LoadEstimator
import org.apache.wayang.core.types.DataSetType
import org.apache.wayang.core.util.ReflectionUtils

import scala.reflect.ClassTag

/**
 * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.CoGroupOperator]]s.
 *
 * @param inputDataQuanta0 [[DataQuantaBuilder]] for the first input [[DataQuanta]]
 * @param inputDataQuanta1 [[DataQuantaBuilder]] for the first input [[DataQuanta]]
 * @param keyUdf0          first key extraction UDF for the [[org.apache.wayang.basic.operators.CoGroupOperator]]
 * @param keyUdf1          first key extraction UDF for the [[org.apache.wayang.basic.operators.CoGroupOperator]]
 */
class CoGroupDataQuantaBuilder[In0, In1, Key](inputDataQuanta0: DataQuantaBuilder[_, In0],
                                              inputDataQuanta1: DataQuantaBuilder[_, In1],
                                              keyUdf0: SerializableFunction[In0, Key],
                                              keyUdf1: SerializableFunction[In1, Key])
                                             (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[CoGroupDataQuantaBuilder[In0, In1, Key], WT2[java.lang.Iterable[In0], java.lang.Iterable[In1]]] {

  /** [[ClassTag]] or surrogate of [[Key]] */
  implicit var keyTag: ClassTag[Key] = _

  /** [[LoadEstimator]] to estimate the CPU load of the [[keyUdf0]]. */
  private var keyUdf0CpuEstimator: LoadEstimator = _

  /** [[LoadEstimator]] to estimate the RAM load of the [[keyUdf0]]. */
  private var keyUdf0RamEstimator: LoadEstimator = _

  /** [[LoadEstimator]] to estimate the CPU load of the [[keyUdf1]]. */
  private var keyUdf1CpuEstimator: LoadEstimator = _

  /** [[LoadEstimator]] to estimate the RAM load of the [[keyUdf1]]. */
  private var keyUdf1RamEstimator: LoadEstimator = _

  // Try to infer the type classes from the UDFs.
  locally {
    val parameters = ReflectionUtils.getTypeParameters(keyUdf0.getClass, classOf[SerializableFunction[_, _]])
    parameters.get("Input") match {
      case cls: Class[In0] => inputDataQuanta0.outputTypeTrap.dataSetType = DataSetType.createDefault(cls)
      case _ => logger.warn("Could not infer types from {}.", keyUdf0)
    }

    this.keyTag = parameters.get("Output") match {
      case cls: Class[Key] => ClassTag(cls)
      case _ =>
        logger.warn("Could not infer types from {}.", keyUdf0)
        ClassTag(DataSetType.none.getDataUnitType.getTypeClass)
    }
  }
  locally {
    val parameters = ReflectionUtils.getTypeParameters(keyUdf1.getClass, classOf[SerializableFunction[_, _]])
    parameters.get("Input") match {
      case cls: Class[In1] => inputDataQuanta1.outputTypeTrap.dataSetType = DataSetType.createDefault(cls)
      case _ => logger.warn("Could not infer types from {}.", keyUdf0)
    }

    this.keyTag = parameters.get("Output") match {
      case cls: Class[Key] => ClassTag(cls)
      case _ =>
        logger.warn("Could not infer types from {}.", keyUdf0)
        ClassTag(DataSetType.none.getDataUnitType.getTypeClass)
    }
  }
  // Since we are currently not looking at type parameters, we can statically determine the output type.
  locally {
    this.outputTypeTrap.dataSetType = dataSetType[WT2[_, _]]
  }

  /**
   * Set a [[LoadEstimator]] for the CPU load of the first key extraction UDF. Currently effectless.
   *
   * @param udfCpuEstimator the [[LoadEstimator]]
   * @return this instance
   */
  def withThisKeyUdfCpuEstimator(udfCpuEstimator: LoadEstimator) = {
    this.keyUdf0CpuEstimator = udfCpuEstimator
    this
  }

  /**
   * Set a [[LoadEstimator]] for the RAM load of first the key extraction UDF. Currently effectless.
   *
   * @param udfRamEstimator the [[LoadEstimator]]
   * @return this instance
   */
  def withThisKeyUdfRamEstimator(udfRamEstimator: LoadEstimator) = {
    this.keyUdf0RamEstimator = udfRamEstimator
    this
  }

  /**
   * Set a [[LoadEstimator]] for the CPU load of the second key extraction UDF. Currently effectless.
   *
   * @param udfCpuEstimator the [[LoadEstimator]]
   * @return this instance
   */
  def withThatKeyUdfCpuEstimator(udfCpuEstimator: LoadEstimator) = {
    this.keyUdf1CpuEstimator = udfCpuEstimator
    this
  }

  /**
   * Set a [[LoadEstimator]] for the RAM load of the second key extraction UDF. Currently effectless.
   *
   * @param udfRamEstimator the [[LoadEstimator]]
   * @return this instance
   */
  def withThatKeyUdfRamEstimator(udfRamEstimator: LoadEstimator) = {
    this.keyUdf1RamEstimator = udfRamEstimator
    this
  }

  override protected def build =
    inputDataQuanta0.dataQuanta().coGroupJava(keyUdf0, inputDataQuanta1.dataQuanta(), keyUdf1)(inputDataQuanta1.classTag, this.keyTag)

}
