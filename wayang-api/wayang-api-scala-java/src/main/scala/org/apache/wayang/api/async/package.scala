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

package org.apache.wayang.api

import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.wayang.api.serialization.TempFileUtils
import org.apache.wayang.core.api.exception.WayangException

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

package object async {

  val logger: Logger = LogManager.getLogger(getClass)

  /**
   * Runs the given data quanta asynchronously with a temporary file as the output.
   *
   * @param dataQuanta  The data quanta to be executed.
   * @param tempFileOut The path to the temporary output file.
   * @tparam Out The type parameter of the data quanta and the output.
   * @return A future representing the completion of the execution, which will contain a
   *         DataQuantaAsyncResult object that holds the path to the temporary output file
   *         and the class tag for the output type.
   */
  def runAsyncWithTempFileOut[Out: ClassTag](dataQuanta: DataQuanta[Out], tempFileOut: String): Future[Unit] = {
    runAsyncWithObjectFileOut(dataQuanta, tempFileOut)
  }


  /**
   * Runs the given DataQuanta asynchronously and writes the output to a text file.
   *
   * @param dataQuanta the DataQuanta to be executed
   * @param url        the URL of the text file to write the output to
   * @tparam Out the type of the output DataQuanta
   * @return a Future representing the completion of the execution
   * @throws WayangException if the WayangContext is not of type MultiContext
   */
  def runAsyncWithTextFileOut[Out: ClassTag](dataQuanta: DataQuanta[Out], url: String): Future[Unit] = {
    // Add sink to multi context and then pass to runAsyncBody
    val wayangContext = dataQuanta.planBuilder.wayangContext
    wayangContext match {
      case context: MultiContext =>
        val updatedContext = context.withTextFileSink(url)
        runAsyncBody(dataQuanta, updatedContext)
      case _ =>
        throw new WayangException("WayangContext is not of type MultiContext")
    }
  }


  /**
   * Runs the given DataQuanta asynchronously and writes the output to an object file specified by the URL.
   *
   * @param dataQuanta the DataQuanta to be executed
   * @param url        the URL of the object file to write the output to
   * @tparam Out the type parameter for the output DataQuanta
   * @return a Future that represents the execution of the DataQuanta
   * @throws WayangException if the WayangContext is not of type MultiContext
   */
  def runAsyncWithObjectFileOut[Out: ClassTag](dataQuanta: DataQuanta[Out], url: String): Future[Unit] = {
    // Add sink to multi context and then pass to runAsyncBody
    val wayangContext = dataQuanta.planBuilder.wayangContext
    wayangContext match {
      case context: MultiContext =>
        val updatedContext = context.withObjectFileSink(url)
        runAsyncBody(dataQuanta, updatedContext)
      case _ =>
        throw new WayangException("WayangContext is not of type MultiContext")
    }
  }


  def runAsyncBody[Out: ClassTag](dataQuanta: DataQuanta[Out], multiContext: MultiContext): Future[Unit] = Future {

    import scala.concurrent.blocking

    // Write plan builder to temp file
    val planBuilderPath = TempFileUtils.writeToTempFileAsString(dataQuanta.planBuilder.withUdfJarsOf(this.getClass))

    // Write operator to temp file
    val operatorPath = TempFileUtils.writeToTempFileAsString(dataQuanta.operator)

    var process: Process = null

    try {
      val mainClass = "org.apache.wayang.api.async.Main"
      val classpath = System.getProperty("java.class.path") // get classpath from parent JVM

      // Child process
      val processBuilder = new ProcessBuilder(
        "java",
        "-cp",
        classpath,
        mainClass,
        operatorPath.toString,
        planBuilderPath.toString
      )

      // Redirect children output to parent output
      processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT)
      processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT)

      // Start child process
      process = processBuilder.start()

      // And block this future while waiting for it
      blocking {
        process.waitFor()
      }
    }

    finally {
      Files.deleteIfExists(planBuilderPath)
      Files.deleteIfExists(operatorPath)
    }

  }

}
