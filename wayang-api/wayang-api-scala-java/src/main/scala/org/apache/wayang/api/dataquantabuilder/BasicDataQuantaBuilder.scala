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

import de.hpi.isg.profiledb.store.model.Experiment
import org.apache.wayang.api.util.TypeTrap
import org.apache.wayang.api.{DataQuanta, DataQuantaBuilder, JavaPlanBuilder}
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator
import org.apache.wayang.core.platform.Platform
import org.apache.wayang.core.types.DataSetType
import org.apache.wayang.core.util.{Logging, ReflectionUtils}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
 * Abstract base class for builders of [[DataQuanta]]. The purpose of the builders is to provide a convenient
 * Java API for Wayang that compensates for lacking default and named arguments.
 */
abstract class BasicDataQuantaBuilder[This <: DataQuantaBuilder[_, Out], Out](implicit _javaPlanBuilder: JavaPlanBuilder)
  extends Logging with DataQuantaBuilder[This, Out] {

  /**
   * Lazy-initialized. The [[DataQuanta]] product of this builder.
   */
  private var result: DataQuanta[Out] = _

  /**
   * A name for the [[DataQuanta]] to be built.
   */
  private var name: String = _

  /**
   * An [[Experiment]] for the [[DataQuanta]] to be built.
   */
  private var experiment: Experiment = _

  /**
   * Broadcasts for the [[DataQuanta]] to be built.
   */
  private val broadcasts: ListBuffer[(String, DataQuantaBuilder[_, _])] = ListBuffer()

  /**
   * [[CardinalityEstimator]] for the [[DataQuanta]] to be built.
   */
  private var cardinalityEstimator: CardinalityEstimator = _

  /**
   * Target [[Platform]]s for the [[DataQuanta]] to be built.
   */
  private val targetPlatforms: ListBuffer[Platform] = ListBuffer()

  /**
   * Paths of UDF JAR files for the [[DataQuanta]] to be built.
   */
  private val udfJars: ListBuffer[String] = ListBuffer()

  /**
   * The type of the [[DataQuanta]] to be built.
   */
  protected[api] val outputTypeTrap = getOutputTypeTrap

  /**
   * Retrieve an intialization value for [[outputTypeTrap]].
   *
   * @return the [[TypeTrap]]
   */
  protected def getOutputTypeTrap = new TypeTrap

  override protected[api] implicit def javaPlanBuilder = _javaPlanBuilder

  override def withName(name: String): This = {
    this.name = name
    this.asInstanceOf[This]
  }

  override def withExperiment(experiment: Experiment): This = {
    this.experiment = experiment
    this.asInstanceOf[This]
  }

  override def withOutputType(outputType: DataSetType[Out]): This = {
    this.outputTypeTrap.dataSetType = outputType
    this.asInstanceOf[This]
  }

  override def withOutputClass(cls: Class[Out]): This = this.withOutputType(DataSetType.createDefault(cls))

  override def withBroadcast[Sender <: DataQuantaBuilder[_, _]](sender: Sender, broadcastName: String): This = {
    this.broadcasts += Tuple2(broadcastName, sender)
    this.asInstanceOf[This]
  }

  override def withCardinalityEstimator(cardinalityEstimator: CardinalityEstimator): This = {
    this.cardinalityEstimator = cardinalityEstimator
    this.asInstanceOf[This]
  }

  override def withTargetPlatform(platform: Platform): This = {
    this.targetPlatforms += platform
    this.asInstanceOf[This]
  }

  def withUdfJarOf(cls: Class[_]): This = this.withUdfJar(ReflectionUtils.getDeclaringJar(cls))

  override def withUdfJar(path: String): This = {
    this.udfJars += path
    this.asInstanceOf[This]
  }

  override protected[api] implicit def classTag: ClassTag[Out] = ClassTag(outputTypeTrap.typeClass)

  override protected[api] def dataQuanta(): DataQuanta[Out] = {
    if (this.result == null) {
      this.result = this.build
      if (this.name != null) this.result.withName(this.name)
      if (this.cardinalityEstimator != null) this.result.withCardinalityEstimator(this.cardinalityEstimator)
      if (this.experiment != null) this.result.withExperiment(experiment)
      this.result.withUdfJars(this.udfJars: _*)
      this.result.withTargetPlatforms(this.targetPlatforms: _*)
      this.broadcasts.foreach {
        case (broadcastName, senderBuilder) => this.result.withBroadcast(senderBuilder.dataQuanta(), broadcastName)
      }
    }
    this.result
  }

  /**
   * Create the [[DataQuanta]] built by this instance. Note the configuration being done in [[dataQuanta()]].
   *
   * @return the created and partially configured [[DataQuanta]]
   */
  protected def build: DataQuanta[Out]

}
