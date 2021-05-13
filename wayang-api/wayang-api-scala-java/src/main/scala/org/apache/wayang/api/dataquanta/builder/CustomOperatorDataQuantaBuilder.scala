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
import org.apache.wayang.api.dataquanta.{DataQuanta, DataQuantaBuilder}
import org.apache.wayang.api.util.DataQuantaBuilderCache
import org.apache.wayang.core.plan.wayangplan.{Operator, OutputSlot, WayangPlan}

/**
 * [[DataQuantaBuilder]] implementation for any [[org.apache.wayang.core.plan.wayangplan.Operator]]s. Does not offer
 * any convenience methods, though.
 *
 * @param operator        the custom [[org.apache.wayang.core.plan.wayangplan.Operator]]
 * @param outputIndex     index of the [[OutputSlot]] addressed by the new instance
 * @param buildCache      a [[DataQuantaBuilderCache]] that must be shared across instances addressing the same [[Operator]]
 * @param inputDataQuanta [[DataQuantaBuilder]]s for the input [[DataQuanta]]
 * @param javaPlanBuilder the [[JavaPlanBuilder]] used to construct the current [[WayangPlan]]
 */
class CustomOperatorDataQuantaBuilder[T](operator: Operator,
                                         outputIndex: Int,
                                         buildCache: DataQuantaBuilderCache,
                                         inputDataQuanta: DataQuantaBuilder[_, _]*)
                                        (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[DataQuantaBuilder[_, T], T] {

  override protected def build = {
    // If the [[operator]] has multiple [[OutputSlot]]s, make sure that we only execute the build once.
    if (!buildCache.hasCached) {
      val dataQuanta = javaPlanBuilder.planBuilder.customOperator(operator, inputDataQuanta.map(_.dataQuanta()): _*)
      buildCache.cache(dataQuanta)
    }
    buildCache(outputIndex)
  }

}
