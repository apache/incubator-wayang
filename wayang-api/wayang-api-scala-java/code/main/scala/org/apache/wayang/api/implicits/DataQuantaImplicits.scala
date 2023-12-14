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


package org.apache.wayang.api.implicits

import org.apache.wayang.api.{BlossomContext, DataQuanta, MultiContextDataQuanta, MultiContextPlanBuilder}

import java.nio.file.Files
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag


class DataQuantaImplicits {}

object DataQuantaImplicits {

  implicit class DataQuantaRunAsyncImplicit[Out: ClassTag](dataQuanta: DataQuanta[Out]) {
    def runAsync(tempFileOut: String): Future[DataQuantaRunAsyncResult[Out]] = Future {
      println(s"Running asynchronously with param: $tempFileOut")

      val wayangContext = dataQuanta.planBuilder.wayangContext

      wayangContext match {
        case context: BlossomContext =>
          val blossomContext = context.withObjectFileSink(tempFileOut)
          println(s"Just added object file sink ${tempFileOut}")

          // Write context to temp file
          // TODO: UDF Jars?
          val multiContextPlanBuilderPath = MultiContextDataQuanta.writeToTempFileAsString(
            new MultiContextPlanBuilder(List(blossomContext)).withUdfJarsOf(classOf[DataQuantaImplicits])
          )

          // Write operator to temp file
          val operatorPath = MultiContextDataQuanta.writeToTempFileAsString(dataQuanta.operator)

          // Child process
          println(s"About to start a process with args ${(operatorPath, multiContextPlanBuilderPath)}")
          val wayangHome = System.getenv("WAYANG_HOME")
          val processBuilder = new ProcessBuilder(
            s"$wayangHome/bin/wayang-submit",
            "org.apache.wayang.api.MultiContextDataQuanta",
            operatorPath.toString,
            multiContextPlanBuilderPath.toString)

          // Redirect children out to parent out
          processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT)
          processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT)

          // Start child process
          val process = processBuilder.start()

          // And block this future while waiting for it
          process.waitFor()

          // Delete temp files
          Files.deleteIfExists(multiContextPlanBuilderPath)
          Files.deleteIfExists(operatorPath)

          // And return
          DataQuantaRunAsyncResult(tempFileOut, implicitly[ClassTag[Out]])

        case _ =>
          throw new Exception("runAsync: WayangContext is not of type BlossomContext")
      }

    }
  }

}