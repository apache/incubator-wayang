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

import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction
import org.apache.wayang.basic.data.{Tuple2 => WayangTuple2}

import scala.reflect.ClassTag

/**
 * This class provides operations on [[DataQuanta]] with additional operations.
 */
class KeyedDataQuanta[Out: ClassTag, Key: ClassTag](val dataQuanta: DataQuanta[Out],
                                                    val keyExtractor: SerializableFunction[Out, Key]) {

  /**
   * Performs a join. The join fields are governed by the [[KeyedDataQuanta]]'s keys.
   *
   * @param that the other [[KeyedDataQuanta]] to join with
   * @return the join product [[DataQuanta]]
   */
  def join[ThatOut: ClassTag](that: KeyedDataQuanta[ThatOut, Key]):
  DataQuanta[WayangTuple2[Out, ThatOut]] =
    dataQuanta.joinJava[ThatOut, Key](this.keyExtractor, that.dataQuanta, that.keyExtractor)

  /**
   * Performs a co-group. The grouping fields are governed by the [[KeyedDataQuanta]]'s keys.
   *
   * @param that the other [[KeyedDataQuanta]] to co-group with
   * @return the co-grouped [[DataQuanta]]
   */
  def coGroup[ThatOut: ClassTag](that: KeyedDataQuanta[ThatOut, Key]):
  DataQuanta[WayangTuple2[java.lang.Iterable[Out], java.lang.Iterable[ThatOut]]] =
    dataQuanta.coGroupJava[ThatOut, Key](this.keyExtractor, that.dataQuanta, that.keyExtractor)

}
