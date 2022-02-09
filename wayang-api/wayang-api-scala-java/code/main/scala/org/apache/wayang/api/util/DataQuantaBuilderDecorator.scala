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

package org.apache.wayang.api.util

import org.apache.wayang.api.{DataQuanta, DataQuantaBuilder, JavaPlanBuilder}
import org.apache.wayang.commons.util.profiledb.model.Experiment
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator
import org.apache.wayang.core.platform.Platform
import org.apache.wayang.core.types.DataSetType

/**
  * Utility to extend a [[DataQuantaBuilder]]'s functionality by decoration.
  */
/**
 * TODO: add the documentation in the methods of org.apache.wayang.api.util.DataQuantaBuilderDecorator
 * labels: documentation,todo
 */
abstract class DataQuantaBuilderDecorator[This <: DataQuantaBuilder[This, Out], Out]
(baseBuilder: DataQuantaBuilder[_, Out])
  extends DataQuantaBuilder[This, Out] {

  /**
    * The type of the [[DataQuanta]] to be built.
    */
  override protected[api] def outputTypeTrap: TypeTrap = baseBuilder.outputTypeTrap

  /**
    * Provide a [[JavaPlanBuilder]] to which this instance is associated.
    */
  override protected[api] implicit def javaPlanBuilder: JavaPlanBuilder = baseBuilder.javaPlanBuilder

  override def withName(name: String): This = {
    baseBuilder.withName(name)
    this.asInstanceOf[This]
  }

  override def withExperiment(experiment: Experiment): This = {
    baseBuilder.withExperiment(experiment)
    this.asInstanceOf[This]
  }

  override def withOutputType(outputType: DataSetType[Out]): This = {
    baseBuilder.withOutputType(outputType)
    this.asInstanceOf[This]
  }

  override def withOutputClass(cls: Class[Out]): This = {
    baseBuilder.withOutputClass(cls)
    this.asInstanceOf[This]
  }

  override def withBroadcast[Sender <: DataQuantaBuilder[_, _]](sender: Sender, broadcastName: String): This = {
    baseBuilder.withBroadcast(sender, broadcastName)
    this.asInstanceOf[This]
  }

  override def withCardinalityEstimator(cardinalityEstimator: CardinalityEstimator): This = {
    baseBuilder.withCardinalityEstimator(cardinalityEstimator)
    this.asInstanceOf[This]
  }

  override def withTargetPlatform(platform: Platform): This = {
    baseBuilder.withTargetPlatform(platform)
    this.asInstanceOf[This]
  }

  override def withUdfJarOf(cls: Class[_]): This = {
    baseBuilder.withUdfJarOf(cls)
    this.asInstanceOf[This]
  }

  override def withUdfJar(path: String): This = {
    baseBuilder.withUdfJar(path)
    this.asInstanceOf[This]
  }

  override protected[api] def dataQuanta(): DataQuanta[Out] = baseBuilder.dataQuanta()
}

