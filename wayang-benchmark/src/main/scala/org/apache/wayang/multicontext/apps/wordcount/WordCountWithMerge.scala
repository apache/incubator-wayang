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
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.java.Java
import org.apache.wayang.multicontext.apps.loadConfig
import org.apache.wayang.spark.Spark

object WordCountWithMerge {

  def main(args: Array[String]): Unit = {
    println("WordCountWithMerge")
    println("Scala version:")
    println(scala.util.Properties.versionString)

    val (configuration1, configuration2) = loadConfig(args)

    val context1 = new MultiContext(configuration1)
      .withPlugin(Java.basicPlugin())
      .withMergeFileSink("file:///tmp/out11")   // The mergeContext will read the output of context 1 from here
    val context2 = new MultiContext(configuration2)
      .withPlugin(Java.basicPlugin())
      .withMergeFileSink("file:///tmp/out12")   // The mergeContext will read the output of context 2 from here

    val multiContextPlanBuilder = new MultiContextPlanBuilder(List(context1, context2))
      .withUdfJarsOf(this.getClass)

    // To be used after merging the previous two
    val mergeContext = new WayangContext(new Configuration())
      .withPlugin(Java.basicPlugin())

    // Generate some test data
    val inputValues1 = Array("Big data is big.", "Is data big data?")
    val inputValues2 = Array("Big big data is big big.", "Is data big data big?")

    // Build and execute a word count in 2 different contexts
    multiContextPlanBuilder
      .loadCollection(context1, inputValues1)
      .loadCollection(context2, inputValues2)
      .forEach(_.flatMap(_.split("\\s+")))
      .forEach(_.map(_.replaceAll("\\W+", "").toLowerCase))
      .forEach(_.map((_, 1)))
      .forEach(_.reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2)))

      // Merge contexts with union operator
      .mergeUnion(mergeContext)

      // Continue processing merged DataQuanta
      .filter(_._2 >= 3)
      .reduceByKey(_._1, (t1, t2) => (t1._1, t1._2 + t2._2))

      // Write out
      // Writes:
      //    (big,9)
      //    (data,6)
      .writeTextFile("file:///tmp/out1.merged", s => s.toString())

  }

}
