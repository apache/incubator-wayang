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


package org.apache.wayang.async.apps

import org.apache.wayang.api.async.DataQuantaImplicits._
import org.apache.wayang.api.async.PlanBuilderImplicits._
import org.apache.wayang.api.{MultiContext, DataQuanta, PlanBuilder}
import org.apache.wayang.java.Java

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object WordCount {

  def main(args: Array[String]): Unit = {
    println("WordCount")
    println("Scala version:")
    println(scala.util.Properties.versionString)

    val planBuilder1 = new PlanBuilder(new MultiContext().withPlugin(Java.basicPlugin())).withUdfJarsOf(this.getClass)
    val planBuilder2 = new PlanBuilder(new MultiContext().withPlugin(Java.basicPlugin())).withUdfJarsOf(this.getClass)
    val planBuilder3 = new PlanBuilder(new MultiContext().withPlugin(Java.basicPlugin())).withUdfJarsOf(this.getClass)

    val result1 = planBuilder1
      .loadCollection(List(1, 2, 3, 4, 5))
      .map(_ * 1)
      .runAsync(tempFileOut = "file:///tmp/out1.temp")

    val result2 = planBuilder2
      .loadCollection(List(6, 7, 8, 9, 10))
      .filter(_ <= 8)
      .runAsync(tempFileOut = "file:///tmp/out2.temp")

    val dq1: DataQuanta[Int] = planBuilder1.loadAsync(result1)
    val dq2: DataQuanta[Int] = planBuilder1.loadAsync(result2)
    val result3 = dq1.union(dq2)
      .map(_ * 3)
      .filter(_ < 100)
      .runAsync(tempFileOut = "file:///tmp/out3.temp", result1, result2)

    val result4 = planBuilder3
      .loadCollection(List(1, 2, 3, 4, 5))
      .filter(_ >= 2)
      .runAsync(tempFileOut = "file:///tmp/out4.temp")

    val dq3: DataQuanta[Int] = planBuilder1.loadAsync(result3)
    val dq4: DataQuanta[Int] = planBuilder1.loadAsync(result4)
    val result5 = dq3.intersect(dq4)
      .map(_ * 4)
      .writeTextFileAsync(url = "file:///tmp/out5.final", result3, result4)

    Await.result(result5, Duration.Inf)
    println("DONE!")

  }

}

