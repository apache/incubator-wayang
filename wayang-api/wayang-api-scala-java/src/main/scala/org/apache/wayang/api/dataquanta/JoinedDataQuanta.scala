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

package org.apache.wayang.api.dataquanta

import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator
import org.apache.wayang.basic.data.{Tuple2 => WayangTuple2}

import scala.reflect.ClassTag

/**
 * This class amends joined [[DataQuanta]] with additional operations.
 */
class JoinedDataQuanta[Out0: ClassTag, Out1: ClassTag]
(val dataQuanta: DataQuanta[WayangTuple2[Out0, Out1]]) {

  /**
   * Assembles a new element from a join product tuple.
   *
   * @param udf     creates the output data quantum from two joinable data quanta
   * @param udfLoad optional [[LoadProfileEstimator]] for the `udf`
   * @return the join product [[DataQuanta]]
   */
  def assemble[NewOut: ClassTag](udf: (Out0, Out1) => NewOut,
                                 udfLoad: LoadProfileEstimator = null):
  DataQuanta[NewOut] =
    dataQuanta.map(joinTuple => udf.apply(joinTuple.field0, joinTuple.field1), udfLoad)

  /**
   * Assembles a new element from a join product tuple.
   *
   * @param assembler creates the output data quantum from two joinable data quanta
   * @param udfLoad   optional [[LoadProfileEstimator]] for the `udf`
   * @return the join product [[DataQuanta]]
   */
  def assembleJava[NewOut: ClassTag](assembler: JavaBiFunction[Out0, Out1, NewOut],
                                     udfLoad: LoadProfileEstimator = null): DataQuanta[NewOut] =
    dataQuanta.map(join => assembler.apply(join.field0, join.field1), udfLoad)

}
