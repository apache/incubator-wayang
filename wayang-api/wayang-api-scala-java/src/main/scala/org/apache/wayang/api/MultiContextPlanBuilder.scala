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

package org.apache.wayang.api

import org.apache.wayang.core.util.ReflectionUtils

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.reflect.ClassTag

class MultiContextPlanBuilder(private[api] val blossomContexts: List[BlossomContext]) {

  private[api] var udfJars = scala.collection.mutable.Set[String]()

  private val blossomContextMap: Map[Long, BlossomContext] = blossomContexts.map(context => context.id -> context).toMap

  private var dataQuantaMap: Map[Long, DataQuanta[_]] = Map()

  private var planBuilderMap: Map[Long, PlanBuilder] = blossomContexts.map(context => context.id -> new PlanBuilder(context)).toMap

  def this(blossomContexts: java.util.List[BlossomContext]) =
    this(blossomContexts.asScala.toList)


  /**
   * Defines user-code JAR files that might be needed to transfer to execution platforms.
   *
   * @param paths paths to JAR files that should be transferred
   * @return this instance
   */
  def withUdfJars(paths: String*): MultiContextPlanBuilder = {
    // For each planBuilder in planBuilderMap,
    // call its `withUdfJars` method with the value of `this.udfJars` and update map with the new PlanBuilder
    planBuilderMap = planBuilderMap.mapValues(_.withUdfJars(paths.toSeq: _*))
    this
  }

  /**
   * Defines user-code JAR files that might be needed to transfer to execution platforms.
   *
   * @param classes whose JAR files should be transferred
   * @return this instance
   */
  def withUdfJarsOf(classes: Class[_]*): MultiContextPlanBuilder = {
    withUdfJars(classes.map(ReflectionUtils.getDeclaringJar).filterNot(_ == null): _*)
    this
  }

  /**
   * Applies a function `f` to each [[PlanBuilder]] in the contexts.
   * Returns a [[MultiContextDataQuanta]] containing the [[DataQuanta]].
   *
   * @param f the function to apply to each [[PlanBuilder]]
   * @tparam Out the type parameter for the output of the `f` function
   * @return a [[MultiContextDataQuanta]] containing the results of applying `f` to each [[PlanBuilder]]
   */
  def forEach[Out: ClassTag](f: PlanBuilder => DataQuanta[Out]): MultiContextDataQuanta[Out] = {
    val dataQuantaMap = blossomContexts.map(context => context.id -> f(planBuilderMap(context.id))).toMap
    new MultiContextDataQuanta[Out](dataQuantaMap)(this)
  }

  /**
   * Same as [[PlanBuilder.readTextFile]], but for specified `blossomContext`
   *
   * @param blossomContext The Blossom context.
   * @param url            The URL of the text file to be read.
   * @return The ReadTextFileMultiContextPlanBuilder with the added data quanta.
   */
  def readTextFile(blossomContext: BlossomContext, url: String): ReadTextFileMultiContextPlanBuilder = {
    dataQuantaMap += (blossomContext.id -> planBuilderMap(blossomContext.id).readTextFile(url))
    new ReadTextFileMultiContextPlanBuilder(this, blossomContextMap, dataQuantaMap.asInstanceOf[Map[Long, DataQuanta[String]]])
  }

  /**
   * Same as [[PlanBuilder.readObjectFile()]], but for specified `blossomContext`
   *
   * @param blossomContext The Blossom context.
   * @param url            The URL of the object file to be read.
   * @return The ReadObjectFileMultiContextPlanBuilder with the added data quanta.
   */
  def readObjectFile[T: ClassTag](blossomContext: BlossomContext, url: String): ReadObjectFileMultiContextPlanBuilder[T] = {
    dataQuantaMap += (blossomContext.id -> planBuilderMap(blossomContext.id).readObjectFile(url))
    new ReadObjectFileMultiContextPlanBuilder[T](this, blossomContextMap, dataQuantaMap.asInstanceOf[Map[Long, DataQuanta[T]]])
  }

  /**
   * Same as [[PlanBuilder.loadCollection]], but for specified `blossomContext`
   *
   * @param blossomContext The Blossom context.
   * @param iterable       The collection to be loaded.
   * @return The LoadCollectionMultiContextPlanBuilder with the added data quanta.
   */
  def loadCollection[T: ClassTag](blossomContext: BlossomContext, iterable: Iterable[T]): LoadCollectionMultiContextPlanBuilder[T] = {
    dataQuantaMap += (blossomContext.id -> planBuilderMap(blossomContext.id).loadCollection(iterable))
    new LoadCollectionMultiContextPlanBuilder[T](this, blossomContextMap, dataQuantaMap.asInstanceOf[Map[Long, DataQuanta[T]]])
  }

}


class ReadTextFileMultiContextPlanBuilder(private val multiContextPlanBuilder: MultiContextPlanBuilder,
                                          private val blossomContextMap: Map[Long, BlossomContext],
                                          private var dataQuantaMap: Map[Long, DataQuanta[String]] = Map()) {

  /**
   * Same as [[PlanBuilder.readTextFile]], but for specified `blossomContext`
   *
   * @param blossomContext The Blossom context.
   * @param url            The URL of the text file to be read.
   * @return The ReadTextFileMultiContextPlanBuilder with the added data quanta.
   */
  def readTextFile(blossomContext: BlossomContext, url: String): ReadTextFileMultiContextPlanBuilder = {
    dataQuantaMap += (blossomContext.id -> blossomContextMap(blossomContext.id).readTextFile(url))
    this
  }
}

object ReadTextFileMultiContextPlanBuilder {
  implicit def toMultiContextDataQuanta(builder: ReadTextFileMultiContextPlanBuilder): MultiContextDataQuanta[String] = {
    new MultiContextDataQuanta[String](builder.dataQuantaMap)(builder.multiContextPlanBuilder)
  }
}


class ReadObjectFileMultiContextPlanBuilder[T: ClassTag](private val multiContextPlanBuilder: MultiContextPlanBuilder,
                                                         private val blossomContextMap: Map[Long, BlossomContext],
                                                         private var dataQuantaMap: Map[Long, DataQuanta[T]] = Map()) {

  /**
   * Same as [[PlanBuilder.readObjectFile()]], but for specified `blossomContext`
   *
   * @param blossomContext The Blossom context.
   * @param url            The URL of the object file to be read.
   * @return The ReadObjectFileMultiContextPlanBuilder with the added data quanta.
   */
  def readObjectFile(blossomContext: BlossomContext, url: String): ReadObjectFileMultiContextPlanBuilder[T] = {
    dataQuantaMap += (blossomContext.id -> blossomContextMap(blossomContext.id).readObjectFile(url))
    this
  }
}

object ReadObjectFileMultiContextPlanBuilder {
  implicit def toMultiContextDataQuanta[T: ClassTag](builder: ReadObjectFileMultiContextPlanBuilder[T]): MultiContextDataQuanta[T] = {
    new MultiContextDataQuanta[T](builder.dataQuantaMap)(builder.multiContextPlanBuilder)
  }
}


class LoadCollectionMultiContextPlanBuilder[T: ClassTag](private val multiContextPlanBuilder: MultiContextPlanBuilder,
                                                         private val blossomContextMap: Map[Long, BlossomContext],
                                                         private var dataQuantaMap: Map[Long, DataQuanta[T]] = Map()) {

  /**
   * Same as [[PlanBuilder.loadCollection]], but for specified `blossomContext`
   *
   * @param blossomContext The Blossom context.
   * @param iterable       The collection to be loaded.
   * @return The LoadCollectionMultiContextPlanBuilder with the added data quanta.
   */
  def loadCollection(blossomContext: BlossomContext, iterable: Iterable[T]): LoadCollectionMultiContextPlanBuilder[T] = {
    dataQuantaMap += (blossomContext.id -> blossomContextMap(blossomContext.id).loadCollection(iterable))
    this
  }
}

object LoadCollectionMultiContextPlanBuilder {
  implicit def toMultiContextDataQuanta[T: ClassTag](builder: LoadCollectionMultiContextPlanBuilder[T]): MultiContextDataQuanta[T] = {
    new MultiContextDataQuanta[T](builder.dataQuantaMap)(builder.multiContextPlanBuilder)
  }
}

