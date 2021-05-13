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

/**
 * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.UnionAllOperator]]s.
 *
 * @param inputDataQuanta0 [[DataQuantaBuilder]] for the first input [[DataQuanta]]
 * @param inputDataQuanta1 [[DataQuantaBuilder]] for the first input [[DataQuanta]]
 */
class UnionDataQuantaBuilder[T](inputDataQuanta0: DataQuantaBuilder[_, T],
                                inputDataQuanta1: DataQuantaBuilder[_, T])
                               (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[UnionDataQuantaBuilder[T], T] {

  override def getOutputTypeTrap = inputDataQuanta0.outputTypeTrap

  override protected def build = inputDataQuanta0.dataQuanta().union(inputDataQuanta1.dataQuanta())

}
