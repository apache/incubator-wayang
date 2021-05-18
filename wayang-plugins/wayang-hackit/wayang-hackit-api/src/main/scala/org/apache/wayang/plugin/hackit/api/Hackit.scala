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

package org.apache.wayang.plugin.hackit.api

import org.apache.wayang.api.PlanBuilder
import org.apache.wayang.api.dataquanta.DataQuanta
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator
import org.apache.wayang.plugin.hackit.core.tagger.HackitTagger
import org.apache.wayang.plugin.hackit.core.tagger.wrapper.FunctionWrapperHackit
import org.apache.wayang.plugin.hackit.core.tags.HackitTag
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple
import org.apache.wayang.api.toSerializableFunction

import scala.language.implicitConversions
import scala.reflect.{ClassTag, classTag}

class Hackit[Key: ClassTag, Type:ClassTag](implicit var planBuilder: PlanBuilder) {

  var dataQuanta: DataQuanta[HackitTuple[Key, Type]] = null

  var tagger : HackitTagger = null;

  def addTag(hackitTag: HackitTag) = {
    println("you are adding a tag")
    this
  }

  def underHackit(dataQuanta: DataQuanta[Type]): Hackit[Key, Type] = {
    val hackit = new Hackit[Key, Type]()
    hackit.dataQuanta = dataQuanta.map[HackitTuple[Key, Type]](
      element => {
        new HackitTuple[Key, Type](element)
      }
    )
    hackit
  }

  def toDataQuanta(): DataQuanta[Type] = {
    this.dataQuanta.map(tuple => tuple.getValue)
  }

  def map[TypeOut: ClassTag](udf: Type => TypeOut, udfLoad: LoadProfileEstimator = null): Hackit[Key, TypeOut] = {
    val hackit = new Hackit[Key, TypeOut]()
    val wrapper = new FunctionWrapperHackit[Key, Type, TypeOut](toSerializableFunction(udf))
    hackit.dataQuanta = dataQuanta.map[HackitTuple[Key, TypeOut]](element => wrapper.apply(element), udfLoad)
    hackit
  }

}


object Hackit {

  //TODO: replace the object with a parameter
  implicit def underHackit[T: ClassTag](dataQuanta: DataQuanta[T]): Hackit[java.lang.Object, T] = {
      return new Hackit[java.lang.Object, T]()(classTag[java.lang.Object], ClassTag(dataQuanta.output.getType.getDataUnitType.getTypeClass), dataQuanta.planBuilder)
                  .underHackit(dataQuanta)
  }

}
