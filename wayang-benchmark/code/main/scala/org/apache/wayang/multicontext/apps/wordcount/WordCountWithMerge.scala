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

import org.apache.wayang.api.{BlossomContext, DataQuanta, MultiContextPlanBuilder}
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.java.Java
import org.apache.wayang.multicontext.apps.loadConfig
import org.apache.wayang.spark.Spark

class WordCountWithMerge {}

object WordCountWithMerge {

  def main(args: Array[String]): Unit = {
    println("Counting words with merge in multi context wayang!")
    println("Scala version:")
    println(scala.util.Properties.versionString)

    val (configuration1, configuration2) = loadConfig(args)

    val context1 = new BlossomContext(configuration1)
      .withPlugin(Java.basicPlugin())
      .withMergeFileSink("file:///tmp/out11")
    val context2 = new BlossomContext(configuration2)
      .withPlugin(Java.basicPlugin())
      .withMergeFileSink("file:///tmp/out12")
    val mergeContext = new WayangContext(new Configuration())
      .withPlugin(Java.basicPlugin())

    val multiContextPlanBuilder = new MultiContextPlanBuilder(List(context1, context2))
      .withUdfJarsOf(classOf[WordCount])

    // Generate some test data
    val inputValues1 = Array("Big data is big.", "Is data big data?")
    val inputValues2 = Array("Big big data is big big.", "Is data big data big?")

    // Build and execute a word count
    multiContextPlanBuilder
      .loadCollection(context1, inputValues1)
      .loadCollection(context2, inputValues2)
      .flatMap(_.split("\\s+"))
      .map(_.replaceAll("\\W+", "").toLowerCase)
      .map((_, 1))
      .reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2))

       .mergeUnion(mergeContext)
       .writeTextFile("file:///tmp/out1.merged", s => s.toString())

//      .mergeJoin[String](mergeContext, _._1)
//      .map(joinList => {
//        val key = joinList.head._1
//        val sum = joinList.map(_._2).sum
//        (key, sum)
//      })
//      .writeTextFile("file:///tmp/out2.merged", s => s.toString())

  }

}
