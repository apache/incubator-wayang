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

import org.apache.wayang.api.async.DataQuantaImplicits._
import org.apache.wayang.api.async.PlanBuilderImplicits._
import org.apache.wayang.api.{BlossomContext, DataQuanta, PlanBuilder}
import org.apache.wayang.java.Java
import org.apache.wayang.multicontext.apps.loadConfig

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class Test {}

object Test {

  def main(args: Array[String]): Unit = {
    println("Counting words in parallel job wayang!")
    println("Scala version:")
    println(scala.util.Properties.versionString)

    val (configuration1, configuration2) = loadConfig(args)

    val context1 = new BlossomContext(configuration1)
      .withPlugin(Java.basicPlugin())
    val context2 = new BlossomContext(configuration2)
      .withPlugin(Java.basicPlugin())

    val planBuilder1 = new PlanBuilder(context1).withUdfJarsOf(classOf[Test])
    val planBuilder2 = new PlanBuilder(context2).withUdfJarsOf(classOf[Test])
    val planBuilder3 = new PlanBuilder(new BlossomContext().withPlugin(Java.basicPlugin())).withUdfJarsOf(classOf[Test])

    /*    val result1 = planBuilder1
          .loadCollection(List(1, 2, 3, 4, 5))
          .map(_ * 1)
          .runAsync(tempFileOut = "file:///tmp/out1.temp")

        val result2 = planBuilder2
          .loadCollection(List(6, 7, 8, 9, 10))
          .filter(_ <= 8)
          .runAsync(tempFileOut = "file:///tmp/out2.temp")

        val result3 = planBuilder1
          .combineFromAsync(result1, result2, (dq1: DataQuanta[Int], dq2: DataQuanta[Int]) => dq1.union(dq2))
          .map(_ * 3)
          .runAsync(tempFileOut = "file:///tmp/out3.temp")

        val result4 = planBuilder3
          .loadCollection(List(1, 2, 3, 4, 5))
          .filter(_ >= 2)
          .runAsync(tempFileOut = "file:///tmp/out4.temp")

        val result5: Future[Unit] = planBuilder1
          .combineFromAsync(result3, result4, (dq1: DataQuanta[Int], dq2: DataQuanta[Int]) => dq1.intersect(dq2))
          .map(_ * 5)
          .writeTextFile("file:///tmp/out5.final", s => s.toString) // Should write 15

        println("Waiting...")
        Await.result(result5, Duration.Inf)*/

    val result1 = planBuilder1.runAsync(_
      .loadCollection(List(1, 2, 3, 4, 5))
      .map(_ * 1),
      tempFileOut = "file:///tmp/out1.temp"
    )

    val result2 = planBuilder2.runAsync(_
      .loadCollection(List(6, 7, 8, 9, 10))
      .filter(_ <= 8),
      tempFileOut = "file:///tmp/out2.temp"
    )

    val result3 = planBuilder1
      .combineFromAsync(result1, result2, (dq1: DataQuanta[Int], dq2: DataQuanta[Int]) => dq1.union(dq2))
      .andThenRunAsync(_
        .map(_ * 3),
        tempFileOut = "file:///tmp/out3.temp"
      )

    val result4 = planBuilder3.runAsync(_
      .loadCollection(List(1, 2, 3, 4, 5))
      .filter(_ >= 2),
      tempFileOut = "file:///tmp/out4.temp"
    )

    val result5: Future[Unit] = planBuilder1
      .combineFromAsync(result3, result4, (dq1: DataQuanta[Int], dq2: DataQuanta[Int]) => dq1.intersect(dq2))
      .map(_ * 5)
      .writeTextFile("file:///tmp/out5.final", s => s.toString) // Should write 15

    println("Waiting...")
    Await.result(result5, Duration.Inf)

  }

}

