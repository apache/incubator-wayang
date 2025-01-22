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

package org.apache.wayang.api.async

import org.apache.wayang.api.serialization.TempFileUtils
import org.apache.wayang.api.{MultiContext, PlanBuilder}
import org.apache.wayang.basic.operators.{ObjectFileSink, TextFileSink}
import org.apache.wayang.core.api.exception.WayangException
import org.apache.wayang.core.plan.wayangplan.{Operator, WayangPlan}

import java.nio.file.Path

object Main {

  /**
   * Main method for executing a Wayang plan.
   * Used as a spawned process for async operations and the MultiContextPlanBuilder API.
   *
   * @param args Array of command line arguments. Two arguments are expected: paths to the serialized operator and context.
   */
  def main(args: Array[String]): Unit = {
    println("org.apache.wayang.api.async.Main")
    println(args.mkString("Array(", ", ", ")"))

    if (args.length != 2) {
      println("Expected two arguments: paths to the serialized operator and context.")
      System.exit(1)
    }

    // Parse file paths
    val operatorPath = Path.of(args(0))
    val planBuilderPath = Path.of(args(1))

    // Parse operator and multiContextPlanBuilder
    val operator = TempFileUtils.readFromTempFileFromString[Operator](operatorPath)
    val planBuilder = TempFileUtils.readFromTempFileFromString[PlanBuilder](planBuilderPath)

    // Get context
    val context = planBuilder.wayangContext.asInstanceOf[MultiContext]

    // Get udf jars
    val udfJars = planBuilder.udfJars
    println(s"udfJars: $udfJars")

    // Get out output type to create sink with
    val outType = operator.getOutput(0).getType.getDataUnitType.getTypeClass

    // Connect to sink and execute plan
    context.getSink match {
      case Some(textFileSink: MultiContext.TextFileSink) =>
        connectToSinkAndExecutePlan(new TextFileSink(textFileSink.url, outType))

      case Some(objectFileSink: MultiContext.ObjectFileSink) =>
        connectToSinkAndExecutePlan(new ObjectFileSink(objectFileSink.url, outType))

      case None =>
        throw new WayangException("All contexts must be attached to an output sink.")

      case _ =>
        throw new WayangException("Invalid sink..")
    }


    def connectToSinkAndExecutePlan(sink: Operator): Unit = {
      operator.connectTo(0, sink, 0)
      context.execute(new WayangPlan(sink), udfJars.toSeq: _*)
    }
  }
}