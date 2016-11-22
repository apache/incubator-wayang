package org.qcri.rheem.api

import org.qcri.rheem.api.util.DataQuantaBuilderDecorator
import org.qcri.rheem.basic.data.Record
import org.qcri.rheem.basic.function.ProjectionDescriptor
import org.qcri.rheem.basic.operators.MapOperator
import org.qcri.rheem.core.optimizer.costs.LoadEstimator


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
  * [[DataQuantaBuilder]] implementation for [[org.qcri.rheem.basic.operators.MapOperator]]s with
  * [[org.qcri.rheem.basic.function.ProjectionDescriptor]]s.
  *
  * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
  * @param fieldNames      field names for the [[org.qcri.rheem.basic.function.ProjectionDescriptor]]
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
