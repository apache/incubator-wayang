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

import org.apache.wayang.api.PlanBuilder
import org.apache.wayang.core.plan.wayangplan.ElementaryOperator
import org.reflections.Reflections
import org.reflections.scanners.{FieldAnnotationsScanner, MethodParameterScanner, ResourcesScanner, SubTypesScanner}
import org.reflections.util.{ClasspathHelper, ConfigurationBuilder}

import scala.collection.immutable.List
import scala.reflect.ClassTag

/**
 * Because the [[DataQuanta]] could be implemented by several instances, then is need to have a factory
 * that know what is the instance that need to build. To know which is the kind that need to be produced
 * the [[DataQuantaFactory]] read a configuration files and create the right instance
 */
object DataQuantaFactory {

  /**
   * template is the instance of [[DataQuantaCreator]] that will be use in the creation of [[DataQuanta]] instance
   */
  var template: DataQuantaCreator = DataQuantaDefault

  /**
   * set the [[DataQuantaCreator]]
   *
   * @param dataQuantaCreator it will be use as creator when the [[DataQuantaFactory.build()]] is called
   */
  def setTemplate(dataQuantaCreator: DataQuantaCreator) = {
    this.template = dataQuantaCreator

  }

  /**
   * Given the configuration loaded the [[DataQuantaFactory.build()]] the right extender, if not configuration is
   * provided the [[DataQuantaFactory]] will create a [[DataQuantaDefault]] instance
   *
   * @param operator is the operator that will be wrapped
   * @param outputIndex index of the operator that will be used
   * @param planBuilder implicit [[PlanBuilder]]
   * @tparam T type that is process by the operator
   * @return Instance of [[DataQuanta]] depending on the configurations provided
   */
  def build[T:ClassTag](operator: ElementaryOperator, outputIndex: Int = 0)(implicit planBuilder: PlanBuilder): DataQuanta[T] = {
    //TODO validate if the correct way
    this.template.wrap[T](operator, outputIndex)
  }

}