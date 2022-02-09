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

package org.apache.wayang.api
/**
 * TODO: add unitary test to the elements in the file org.apache.wayang.api.RecordDataQuantaBuilder.scala
 * labels: unitary-test,todo
 */
import org.apache.wayang.api.util.DataQuantaBuilderDecorator
import org.apache.wayang.basic.data.Record
import org.apache.wayang.basic.function.ProjectionDescriptor
import org.apache.wayang.basic.operators.MapOperator
import org.apache.wayang.core.optimizer.costs.LoadEstimator


/**
  * Enriches [[DataQuantaBuilder]] by [[Record]]-specific operations.
  */
trait RecordDataQuantaBuilder[+This <: RecordDataQuantaBuilder[This]]
  extends DataQuantaBuilder[This, Record] {

  /**
    * Feed built [[DataQuanta]] into a [[MapOperator]] with a [[ProjectionDescriptor]].
    *
    * @param fieldNames names of the fields to be projected
    * @return a new instance representing the [[MapOperator]]'s output
    */
  def projectRecords(fieldNames: Array[String]): ProjectRecordsDataQuantaBuilder =
  new ProjectRecordsDataQuantaBuilder(this, fieldNames)

}

/**
  * This decorator enriches a regular [[DataQuantaBuilder]] with operations of a [[RecordDataQuantaBuilder]].
  *
  * @param baseBuilder the [[DataQuantaBuilder]] to be enriched
  */
class RecordDataQuantaBuilderDecorator[This <: RecordDataQuantaBuilder[This]]
(baseBuilder: DataQuantaBuilder[_, Record])
  extends DataQuantaBuilderDecorator[This, Record](baseBuilder) with RecordDataQuantaBuilder[This]

/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.MapOperator]]s with
  * [[org.apache.wayang.basic.function.ProjectionDescriptor]]s.
  *
  * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
  * @param fieldNames      field names for the [[org.apache.wayang.basic.function.ProjectionDescriptor]]
  */
class ProjectRecordsDataQuantaBuilder(inputDataQuanta: DataQuantaBuilder[_, Record], fieldNames: Array[String])
                                     (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[ProjectRecordsDataQuantaBuilder, Record] with RecordDataQuantaBuilder[ProjectRecordsDataQuantaBuilder] {

  /** SQL implementation of the projection. */
  private var sqlUdf: String = _

  /** [[LoadEstimator]] to estimate the CPU load of the projection. */
  private var udfCpuEstimator: LoadEstimator = _

  /** [[LoadEstimator]] to estimate the RAM load of the projection. */
  private var udfRamEstimator: LoadEstimator = _

  /**
    * Set a [[LoadEstimator]] for the CPU load of the UDF.
    *
    * @param udfCpuEstimator the [[LoadEstimator]]
    * @return this instance
    */
  def withUdfCpuEstimator(udfCpuEstimator: LoadEstimator) = {
    this.udfCpuEstimator = udfCpuEstimator
    this
  }

  /**
    * Set a [[LoadEstimator]] for the RAM load of the UDF.
    *
    * @param udfRamEstimator the [[LoadEstimator]]
    * @return this instance
    */
  def withUdfRamEstimator(udfRamEstimator: LoadEstimator) = {
    this.udfRamEstimator = udfRamEstimator
    this
  }

  /**
    * Add a SQL implementation of the projection.
    *
    * @param sqlUdf attribute names or `*` that can be plugged into a `SELECT` clause
    * @return this instance
    */
  def withSqlUdf(sqlUdf: String) = {
    this.sqlUdf = sqlUdf
    this
  }

  override protected def build = inputDataQuanta.dataQuanta().projectRecords(fieldNames.toSeq, this.udfCpuEstimator, this.udfRamEstimator)

}
