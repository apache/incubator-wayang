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

import scala.reflect.ClassTag

case class MultiContextPlanBuilder(var contexts: List[BlossomContext]) {

  private[api] var withClassesOf: Seq[Class[_]] = Seq()

  def withUdfJarsOf(classes: Class[_]*): MultiContextPlanBuilder = {
    this.withClassesOf ++= classes
    this
  }

  private def getPlanBuilder: PlanBuilder = new PlanBuilder(new BlossomContext())

  private def wrapInMultiContextDataQuanta[T: ClassTag](f: PlanBuilder => DataQuanta[T]): MultiContextDataQuanta[T] =
    new MultiContextDataQuanta[T](f(getPlanBuilder))(this)

  def readTextFile(url: String): MultiContextDataQuanta[String] =
    wrapInMultiContextDataQuanta(_.readTextFile(url))

  def readObjectFile[T: ClassTag](url: String): MultiContextDataQuanta[T] =
    wrapInMultiContextDataQuanta(_.readObjectFile(url))

  def readTable(source: TableSource): MultiContextDataQuanta[Record] =
    wrapInMultiContextDataQuanta(_.readTable(source))

  def loadCollection[T: ClassTag](collection: java.util.Collection[T]): MultiContextDataQuanta[T] =
    wrapInMultiContextDataQuanta(_.loadCollection(collection))

  def loadCollection[T: ClassTag](iterable: Iterable[T]): MultiContextDataQuanta[T] =
    wrapInMultiContextDataQuanta(_.loadCollection(iterable))

}
