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
import org.apache.wayang.api.util.TypeTrap

/**
 * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.DistinctOperator]]s.
 *
 * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
 */
class DistinctDataQuantaBuilder[T](inputDataQuanta: DataQuantaBuilder[_, T])
                                  (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[DistinctDataQuantaBuilder[T], T] {

  // Reuse the input TypeTrap to enforce type equality between input and output.
  override def getOutputTypeTrap: TypeTrap = inputDataQuanta.outputTypeTrap

  override protected def build = inputDataQuanta.dataQuanta().distinct

}
