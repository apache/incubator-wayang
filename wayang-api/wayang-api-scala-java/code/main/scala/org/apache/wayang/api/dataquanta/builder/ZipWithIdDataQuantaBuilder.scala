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

import org.apache.wayang.api.dataquanta.{DataQuanta, DataQuantaBuilder}
import org.apache.wayang.api.{JavaPlanBuilder, dataSetType}
import org.apache.wayang.basic.data.{Tuple2 => WT2}

/**
 * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.ZipWithIdOperator]]s.
 *
 * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
 */
class ZipWithIdDataQuantaBuilder[T](inputDataQuanta: DataQuantaBuilder[_, T])
                                   (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[ZipWithIdDataQuantaBuilder[T], WT2[java.lang.Long, T]] {

  // Since we are currently not looking at type parameters, we can statically determine the output type.
  locally {
    this.outputTypeTrap.dataSetType = dataSetType[WT2[_, _]]
  }

  override protected def build = inputDataQuanta.dataQuanta().zipWithId

}
