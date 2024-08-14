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


package org.apache.wayang.multicontext.apps.wordcount

import org.apache.wayang.api.{MultiContext, DataQuanta, MultiContextPlanBuilder}
import org.apache.wayang.java.Java
import org.apache.wayang.multicontext.apps.loadConfig

object WordCountCombineEach {

  def main(args: Array[String]): Unit = {
    println("WordCountCombineEach")
    println("Scala version:")
    println(scala.util.Properties.versionString)

    val (configuration1, configuration2) = loadConfig(args)

    val context1 = new MultiContext(configuration1)
      .withPlugin(Java.basicPlugin())
      .withTextFileSink("file:///tmp/out11")
    val context2 = new MultiContext(configuration2)
      .withPlugin(Java.basicPlugin())
      .withTextFileSink("file:///tmp/out12")

    val multiContextPlanBuilder = new MultiContextPlanBuilder(List(context1, context2))
      .withUdfJarsOf(this.getClass)

    // Generate some test data
    val inputValues = Array("Big data is big.", "Is data big data?")

    // Build and execute a word count
    val dq1 = multiContextPlanBuilder
      .forEach(_.loadCollection(inputValues))
      .forEach(_.flatMap(_.split("\\s+")))
      .forEach(_.map(_.replaceAll("\\W+", "").toLowerCase))
      .forEach(_.map((_, 1)))
      .forEach(_.reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2)))

    val dq2 = multiContextPlanBuilder
      .forEach(_.loadCollection(inputValues))
      .forEach(_.flatMap(_.split("\\s+")))
      .forEach(_.map(_.replaceAll("\\W+", "").toLowerCase))
      .forEach(_.map((_, 1)))
      .forEach(_.reduceByKey(_._1, (a, _) => (a._1, 100)))

    dq1.combineEach(dq2, (dq1: DataQuanta[(String, Int)], dq2: DataQuanta[(String, Int)]) => dq1.union(dq2))
      .forEach(_.map(t => (t._1 + " wayang out", t._2)))
      .execute()
  }

}

