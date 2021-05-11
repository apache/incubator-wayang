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
import org.apache.wayang.core.function.FunctionDescriptor.SerializablePredicate
import org.apache.wayang.core.optimizer.costs.{LoadProfile, LoadProfileEstimator}
import org.apache.wayang.core.types.DataSetType
import org.apache.wayang.core.util.{Tuple => WayangTuple}

import java.util.function.{Function => JavaFunction}
import java.util.{Collection => JavaCollection}
import scala.reflect.ClassTag

/**
 * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.DoWhileOperator]]s.
 *
 * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
 * @param conditionUdf    UDF for the looping condition
 * @param bodyBuilder     builds the loop body
 */
class DoWhileDataQuantaBuilder[T, ConvOut](inputDataQuanta: DataQuantaBuilder[_, T],
                                           conditionUdf: SerializablePredicate[JavaCollection[ConvOut]],
                                           bodyBuilder: JavaFunction[DataQuantaBuilder[_, T], WayangTuple[DataQuantaBuilder[_, T], DataQuantaBuilder[_, ConvOut]]])
                                          (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[DoWhileDataQuantaBuilder[T, ConvOut], T] {

  // TODO: Get the ClassTag right.
  implicit private var convOutClassTag: ClassTag[ConvOut] = ClassTag(DataSetType.none.getDataUnitType.getTypeClass)

  // Reuse the input TypeTrap to enforce type equality between input and output.
  override def getOutputTypeTrap: TypeTrap = inputDataQuanta.outputTypeTrap

  // TODO: We could improve by combining the TypeTraps in the body loop.

  /** [[LoadProfileEstimator]] to estimate the [[LoadProfile]] of the UDF. */
  private var udfLoadProfileEstimator: LoadProfileEstimator = _

  /** Number of expected iterations. */
  private var numExpectedIterations = 20

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
   * Explicitly set the [[DataSetType]] for the condition [[DataQuanta]]. Note that it is not
   * always necessary to set it and that it can be inferred in some situations.
   *
   * @param outputType the output [[DataSetType]]
   * @return this instance
   */
  def withConditionType(outputType: DataSetType[ConvOut]) = {
    this.convOutClassTag = ClassTag(outputType.getDataUnitType.getTypeClass)
    this
  }

  /**
   * Explicitly set the [[Class]] for the condition [[DataQuanta]]. Note that it is not
   * always necessary to set it and that it can be inferred in some situations.
   *
   * @param cls the output [[Class]]
   * @return this instance
   */
  def withConditionClass(cls: Class[ConvOut]) = {
    this.convOutClassTag = ClassTag(cls)
    this
  }

  /**
   * Set the number of expected iterations for the built [[org.apache.wayang.basic.operators.DoWhileOperator]].
   *
   * @param numExpectedIterations the expected number of iterations
   * @return this instance
   */
  def withExpectedNumberOfIterations(numExpectedIterations: Int) = {
    this.numExpectedIterations = numExpectedIterations
    this
  }

  override protected def build =
    inputDataQuanta.dataQuanta().doWhileJava[ConvOut](
      conditionUdf, dataQuantaBodyBuilder, this.numExpectedIterations, this.udfLoadProfileEstimator
    )(this.convOutClassTag)


  /**
   * Create a loop body builder that is based on [[DataQuanta]].
   *
   * @return the loop body builder
   */
  private def dataQuantaBodyBuilder =
    new JavaFunction[DataQuanta[T], WayangTuple[DataQuanta[T], DataQuanta[ConvOut]]] {
      override def apply(loopStart: DataQuanta[T]) = {
        val loopStartBuilder = new FakeDataQuantaBuilder(loopStart)
        val loopEndBuilders = bodyBuilder(loopStartBuilder)
        new WayangTuple(loopEndBuilders.field0.dataQuanta(), loopEndBuilders.field1.dataQuanta())
      }
    }

}
