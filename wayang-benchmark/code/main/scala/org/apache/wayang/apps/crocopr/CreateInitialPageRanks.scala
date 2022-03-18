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

package org.apache.wayang.apps.crocopr

import org.apache.wayang.core.function.ExecutionContext
import org.apache.wayang.core.function.FunctionDescriptor.ExtendedSerializableFunction
import org.apache.wayang.core.util.WayangCollections

/**
  * Creates initial page ranks.
  */
class CreateInitialPageRanks(numPagesBroadcast: String)
  extends ExtendedSerializableFunction[(Long, Iterable[Long]), (Long, Double)] {

  private var initialRank: Double = _

  override def open(executionCtx: ExecutionContext): Unit = {
    val numPages = WayangCollections.getSingle(executionCtx.getBroadcast[Long](numPagesBroadcast))
    this.initialRank = 1d / numPages
  }

  override def apply(in: (Long, Iterable[Long])) = (in._1, this.initialRank)

}
