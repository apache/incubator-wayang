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

package org.apache.wayang.api.dataquantabuilder

import org.apache.wayang.api.{DataQuanta, DataQuantaBuilder, JavaPlanBuilder}

import scala.reflect.ClassTag

/**
 * Wraps [[DataQuanta]] and exposes them as [[DataQuantaBuilder]], i.e., this is an adapter.
 *
 * @param _dataQuanta the wrapped [[DataQuanta]]
 */
class FakeDataQuantaBuilder[T](_dataQuanta: DataQuanta[T])(implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[FakeDataQuantaBuilder[T], T] {

  override implicit def classTag = ClassTag(_dataQuanta.output.getType.getDataUnitType.getTypeClass)

  override def dataQuanta() = _dataQuanta

  /**
   * Create the [[DataQuanta]] built by this instance. Note the configuration being done in [[dataQuanta()]].
   *
   * @return the created and partially configured [[DataQuanta]]
   */
  override protected def build: DataQuanta[T] = _dataQuanta
}
