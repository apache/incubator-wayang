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
import org.apache.wayang.api.dataquanta.DataQuantaBuilder
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction

/**
 * This is not an actual [[DataQuantaBuilder]] but rather decorates such a [[DataQuantaBuilder]] with a key.
 */
class KeyedDataQuantaBuilder[Out, Key](private val dataQuantaBuilder: DataQuantaBuilder[_, Out],
                                       private val keyExtractor: SerializableFunction[Out, Key])
                                      (implicit javaPlanBuilder: JavaPlanBuilder) {

  /**
   * Joins this instance with the given one via their keys.
   *
   * @param that the instance to join with
   * @return a [[DataQuantaBuilder]] representing the join product
   */
  def join[ThatOut](that: KeyedDataQuantaBuilder[ThatOut, Key]) =
    dataQuantaBuilder.join(this.keyExtractor, that.dataQuantaBuilder, that.keyExtractor)

  /**
   * Co-groups this instance with the given one via their keys.
   *
   * @param that the instance to join with
   * @return a [[DataQuantaBuilder]] representing the co-group product
   */
  def coGroup[ThatOut](that: KeyedDataQuantaBuilder[ThatOut, Key]) =
    dataQuantaBuilder.coGroup(this.keyExtractor, that.dataQuantaBuilder, that.keyExtractor)

}
