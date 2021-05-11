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
import org.apache.wayang.core.types.DataSetType
import org.apache.wayang.core.util.WayangCollections

import java.util.{Collection => JavaCollection}

/**
 * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.CollectionSource]]s.
 *
 * @param collection      the [[JavaCollection]] to be loaded
 * @param javaPlanBuilder the [[JavaPlanBuilder]]
 */
class LoadCollectionDataQuantaBuilder[Out](collection: JavaCollection[Out])(implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[LoadCollectionDataQuantaBuilder[Out], Out] {

  // Try to infer the type class from the collection.
  locally {
    if (!collection.isEmpty) {
      val any = WayangCollections.getAny(collection)
      if (any != null) {
        this.outputTypeTrap.dataSetType = DataSetType.createDefault(any.getClass)
      }
    }
  }

  override protected def build: DataQuanta[Out] = javaPlanBuilder.planBuilder.loadCollection(collection)(this.classTag)

}
