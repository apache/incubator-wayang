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


package org.apache.wayang.apps.multicontext

import org.apache.wayang.api.{BlossomContext, MultiContextPlanBuilder}
import org.apache.wayang.core.api.Configuration
import org.apache.wayang.java.Java
import org.apache.wayang.spark.Spark

import java.lang.management.ManagementFactory


class Test1 {}

object Test1 {

  def main(args: Array[String]): Unit = {

    println("Multi world 123456!")
    println(scala.util.Properties.versionString)

    var configuration1: Option[Configuration] = None
    var configuration2: Option[Configuration] = None

    if (args.length < 2) {
      println("Loading default configurations.")
      configuration1 = Some(new Configuration())
      configuration2 = Some(new Configuration())
    } else {
      println("Loading custom configurations.")
      configuration1 = Some(loadConfiguration(args(0)))
      configuration2 = Some(loadConfiguration(args(1)))
    }

    val context1 = new BlossomContext(configuration1.get).withPlugin(Spark.basicPlugin()).withTextFileSink("file:///tmp/out11")
    val context2 = new BlossomContext(configuration2.get).withPlugin(Spark.basicPlugin()).withTextFileSink("file:///tmp/out12")

    val multiContextPlanBuilder = MultiContextPlanBuilder(List(context1, context2))
      .withUdfJarsOf(classOf[Test1])

    val beforeUsedMem = Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()
    multiContextPlanBuilder
      .readTextFile("file:///tmp/in1.txt")
      .map(s => s + " Wayang out.")
      .filter(s => s.length > 20)
      .execute2()
    val afterUsedMem = Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()

    val actualMemUsed = afterUsedMem - beforeUsedMem

    println()
    println(s"Used $actualMemUsed")   // Printed "Used 69,681,136", "Used 63,189,776", "Used 36,298,936", "Used 52,618,920"
    println()

  }


  private def loadConfiguration(url: String): Configuration = {
    try {
      new Configuration(url)
    } catch {
      case unexpected: Exception =>
        unexpected.printStackTrace()
        println(s"Can't load configuration from $url")
        new Configuration()
    }
  }
}
