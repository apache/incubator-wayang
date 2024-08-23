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

import org.apache.wayang.api.{MultiContext, MultiContextPlanBuilder}
import org.apache.wayang.multicontext.apps.loadConfig
import org.apache.wayang.java.Java
import org.apache.wayang.spark.Spark

class WordCount {}

object WordCount {

  def main(args: Array[String]): Unit = {
    println("WordCount")
    println("Scala version:")
    println(scala.util.Properties.versionString)

    val (configuration1, configuration2) = loadConfig(args)

    val context1 = new MultiContext(configuration1)
      .withPlugin(Spark.basicPlugin())
      .withTextFileSink("file:///tmp/out11")
    val context2 = new MultiContext(configuration2)
      .withPlugin(Spark.basicPlugin())
      .withTextFileSink("file:///tmp/out12")

    val multiContextPlanBuilder = new MultiContextPlanBuilder(List(context1, context2))
      .withUdfJarsOf(this.getClass)

    // Generate some test data
    val inputValues = Array("Big data is big.", "Is data big data?")


    // Build and execute a word count
    multiContextPlanBuilder.forEach(_
      .loadCollection(inputValues)
      .flatMap(_.split("\\s+"))
      .map(_.replaceAll("\\W+", "").toLowerCase)
      .map((_, 1))
      .reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2))
    ).execute()

    /*.forEach(_.loadCollection(inputValues))
    .forEach(_.flatMap(_.split("\\s+")))
    .forEach(_.map(_.replaceAll("\\W+", "").toLowerCase))
    .forEach(_.map((_, 1)))
    .forEach(_.reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2)))
    .execute()*/

  }

}

