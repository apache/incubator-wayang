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

import org.apache.wayang.basic.data.Record
import org.apache.wayang.basic.function.ProjectionDescriptor
import org.apache.wayang.basic.operators.MapOperator
import org.apache.wayang.basic.types.RecordType
import org.apache.wayang.core.optimizer.costs.LoadEstimator

/**
  * This class enhances the functionality of [[DataQuanta]] with [[Record]]s.
  */
class RecordDataQuanta(dataQuanta: DataQuanta[Record]) {

  implicit def planBuilder: PlanBuilder = dataQuanta.planBuilder

  /**
    * Feed this instance into a [[MapOperator]] with a [[ProjectionDescriptor]].
    *
    * @param fieldNames names of the fields to be projected
    * @param udfCpuLoad optional [[LoadEstimator]] for the CPU consumption of the `udf`
    * @param udfRamLoad optional [[LoadEstimator]] for the RAM consumption of the `udf`
    * @return a new instance representing the [[MapOperator]]'s output
    */
  def projectRecords(fieldNames: Seq[String],
                     udfCpuLoad: LoadEstimator = null,
                     udfRamLoad: LoadEstimator = null): DataQuanta[Record] = {
    val mapOperator = MapOperator.createProjection(
      dataQuanta.output.getType.getDataUnitType.asInstanceOf[RecordType],
      fieldNames: _*
    )
    dataQuanta.connectTo(mapOperator, 0)
    wrap[Record](mapOperator)
  }

}
