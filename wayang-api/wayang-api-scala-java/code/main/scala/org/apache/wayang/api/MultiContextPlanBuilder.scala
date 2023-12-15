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

import org.apache.wayang.basic.data.Record
import org.apache.wayang.basic.operators.TableSource

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.reflect.ClassTag

class MultiContextPlanBuilder(val blossomContexts: List[BlossomContext]) {

  private[api] var withClassesOf: Seq[Class[_]] = Seq()

  private[api] var udfJars = scala.collection.mutable.Set[String]()

  private val blossomContextMap: Map[Long, BlossomContext] = blossomContexts.map(context => context.id -> context).toMap

  private var dataQuantaMap: Map[Long, DataQuanta[_]] = Map()

  def this(blossomContexts: java.util.List[BlossomContext]) =
    this(blossomContexts.asScala.toList)

  def withUdfJars(paths: String*): MultiContextPlanBuilder = {
    this.udfJars ++= paths
    this
  }

  def withUdfJarsOf(classes: Class[_]*): MultiContextPlanBuilder = {
    this.withClassesOf ++= classes
    this
  }

  private def getPlanBuilder(blossomContextId: Long): PlanBuilder = {
    new PlanBuilder(blossomContextMap(blossomContextId))
  }

  private def wrapInMultiContextDataQuanta[T: ClassTag](f: PlanBuilder => DataQuanta[T]): MultiContextDataQuanta[T] = {
    val dataQuantaMap = blossomContexts.map(context => context.id -> f(getPlanBuilder(context.id))).toMap
    new MultiContextDataQuanta[T](dataQuantaMap)(this)
  }

  def readTextFile(url: String): MultiContextDataQuanta[String] =
    wrapInMultiContextDataQuanta(_.readTextFile(url))

  def readTextFile(blossomContext: BlossomContext, url: String): ReadTextFileMultiContextPlanBuilder = {
    dataQuantaMap += (blossomContext.id -> blossomContextMap(blossomContext.id).readTextFile(url))
    new ReadTextFileMultiContextPlanBuilder(this, blossomContextMap, dataQuantaMap.asInstanceOf[Map[Long, DataQuanta[String]]])
  }

  def readObjectFile[T: ClassTag](url: String): MultiContextDataQuanta[T] =
    wrapInMultiContextDataQuanta(_.readObjectFile(url))

  def readObjectFile[T: ClassTag](blossomContext: BlossomContext, url: String): ReadObjectFileMultiContextPlanBuilder[T] = {
    dataQuantaMap += (blossomContext.id -> blossomContextMap(blossomContext.id).readObjectFile(url))
    new ReadObjectFileMultiContextPlanBuilder[T](this, blossomContextMap, dataQuantaMap.asInstanceOf[Map[Long, DataQuanta[T]]])
  }

  def readTable(source: TableSource): MultiContextDataQuanta[Record] =
    wrapInMultiContextDataQuanta(_.readTable(source))

  def loadCollection[T: ClassTag](iterable: Iterable[T]): MultiContextDataQuanta[T] =
    wrapInMultiContextDataQuanta(_.loadCollection(iterable))

  def loadCollection[T: ClassTag](blossomContext: BlossomContext, iterable: Iterable[T]): LoadCollectionMultiContextPlanBuilder[T] = {
    dataQuantaMap += (blossomContext.id -> blossomContextMap(blossomContext.id).loadCollection(iterable))
    new LoadCollectionMultiContextPlanBuilder[T](this, blossomContextMap, dataQuantaMap.asInstanceOf[Map[Long, DataQuanta[T]]])
  }

}


class ReadTextFileMultiContextPlanBuilder(val multiContextPlanBuilder: MultiContextPlanBuilder,
                                          val blossomContextMap: Map[Long, BlossomContext],
                                          var dataQuantaMap: Map[Long, DataQuanta[String]] = Map()) {
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


class ReadObjectFileMultiContextPlanBuilder[T: ClassTag](val multiContextPlanBuilder: MultiContextPlanBuilder,
                                                         val blossomContextMap: Map[Long, BlossomContext],
                                                         var dataQuantaMap: Map[Long, DataQuanta[T]] = Map()) {
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


class LoadCollectionMultiContextPlanBuilder[T: ClassTag](val multiContextPlanBuilder: MultiContextPlanBuilder,
                                                         val blossomContextMap: Map[Long, BlossomContext],
                                                         var dataQuantaMap: Map[Long, DataQuanta[T]] = Map()) {
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

