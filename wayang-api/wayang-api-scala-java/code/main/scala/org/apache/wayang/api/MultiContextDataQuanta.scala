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

import org.apache.wayang.api.serialization.SerializationUtils
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.api.exception.WayangException
import org.apache.wayang.spark.Spark

import java.io.{FileInputStream, FileOutputStream}
import java.nio.file.{Files, Path}
import scala.reflect.ClassTag
import java.util.concurrent.Executors
import scala.concurrent.Future.sequence
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionException, Future}
import scala.util.{Failure, Success}

class MultiContextDataQuanta[Out: ClassTag](val dataQuantaList: List[DataQuanta[Out]], outputIndex: Int = 0)(val multiContextPlanBuilder: MultiContextPlanBuilder) {

  def map[NewOut: ClassTag](udf: Out => NewOut): MultiContextDataQuanta[NewOut] = {
    new MultiContextDataQuanta[NewOut](
      dataQuantaList.map(dataQuanta => dataQuanta.map[NewOut](udf))
    )(multiContextPlanBuilder)
  }

  def filter(udf: Out => Boolean): MultiContextDataQuanta[Out] = {
    new MultiContextDataQuanta[Out](
      dataQuantaList.map(dataQuanta => dataQuanta.filter(udf))
    )(multiContextPlanBuilder)
  }

  /*
    def writeTextFile(urls: List[String], formatterUdf: Out => String): Unit = {
      require(urls.length == operators.length, "The number of text file sinks is not equal to the number of contexts specified.")
      operators.zip(urls).foreach {
        case (operator, url) => operator.writeTextFile(url, formatterUdf)
      }
    }
  */

  def execute(): Unit = {
    dataQuantaList.zip(multiContextPlanBuilder.contexts).foreach {
      case (dataQuanta, context) =>
        context.getSink match {
          case Some(textFileSink: BlossomContext.TextFileSink) => dataQuanta.writeTextFile(textFileSink.textFileUrl, s => s.toString)
          case Some(objectFileSink: BlossomContext.ObjectFileSink) => dataQuanta.writeTextFile(objectFileSink.textFileUrl, s => s.toString)
          case None => throw new WayangException("All contexts must be attached to an output sink.")
        }
    }
  }

  def execute2(): Unit = {
    println("Inside execute2")
    dataQuantaList.zip(multiContextPlanBuilder.contexts).foreach {
      case (dataQuanta, context) =>
        // TODO:
//        val dataQuantaPath = MultiContextDataQuanta.writeToTempFile(dataQuanta)
//        val contextPath = MultiContextDataQuanta.writeToTempFile(context)
        val dataQuantaPath = context.getSink.get.asInstanceOf[BlossomContext.TextFileSink].textFileUrl
        val contextPath = "dummy_value"

        val wayangHome = System.getenv("WAYANG_HOME")

        // Spawn a new process and pass the temp file paths as command-line arguments
        println(s"About to start a process with arg ${dataQuantaPath}")
        val processBuilder = new ProcessBuilder(s"$wayangHome/bin/wayang-submit", "org.apache.wayang.api.MultiContextDataQuanta", dataQuantaPath.toString, contextPath.toString)
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT)
        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT)
        val process = processBuilder.start()
        process.waitFor() // Wait for the process to complete. Remove if asynchronous execution is desired.


      // Cleanup: Delete the temp files (optional if you want to inspect them post-execution)
//        Files.deleteIfExists(dataQuantaPath)
//        Files.deleteIfExists(contextPath)
    }
  }

  /*
  def execute(): Unit = {
    // Create the custom ExecutionContext
    val threadPool = Executors.newFixedThreadPool(operators.size)
    implicit val customExecutionContext: ExecutionContext = ExecutionContext.fromExecutor(threadPool)

    // Run your tasks
    val futures = operators.zip(multiContextPlanBuilder.contexts).map {
      case (operator, context) =>
        Future {
          println(s"Executing on thread: ${Thread.currentThread().getName}")
          context.sink match {
            case Some(textFileSink: BlossomContext.TextFileSink) =>
              operator.writeTextFile(textFileSink.textFileUrl, s => s.toString)
            case Some(objectFileSink: BlossomContext.ObjectFileSink) =>
              operator.writeTextFile(objectFileSink.textFileUrl, s => s.toString)
            case None => throw new WayangException("All contexts must be attached to an output sink.")
          }
        }
    }

    // Wait for all futures to complete
    val combinedFuture = sequence(futures)

    try {
      Await.result(combinedFuture, Duration.Inf)
      println("All plans completed successfully.")
    } catch {
      case e: ExecutionException => throw e.getCause
    } finally {
      threadPool.shutdown() // Ensure the thread pool is always shut down
    }
  }
  */

}


object MultiContextDataQuanta {
  def main(args: Array[String]): Unit = {
    println("args")
    println(args.mkString("Array(", ", ", ")"))
    if (args.length != 2) {
      System.err.println("Expected two arguments: paths to the serialized dataQuanta and context.")
      System.exit(1)
    }

    println(s"New process here about to write to ${args.head}!")

    // TODO:

    val configuration = new Configuration()
    val wayangContext = new WayangContext(configuration).withPlugin(Spark.basicPlugin())
    new PlanBuilder(wayangContext)
      .readTextFile("file:///tmp/in1.txt")
      .map(s => s + " Wayang out.")
      .filter(s => s.length > 20)
      .writeTextFile(args.head, s => s)

//    val dataQuantaPath = Path.of(args(0))
//    val contextPath = Path.of(args(1))
//
//    val dataQuanta = MultiContextDataQuanta.readFromTempFile(dataQuantaPath).asInstanceOf[DataQuanta[Any]]
//    val context = MultiContextDataQuanta.readFromTempFile(contextPath).asInstanceOf[BlossomContext]
//
//    // Now, you can perform your operation as before
//    context.getSink match {
//      case Some(textFileSink: BlossomContext.TextFileSink) => dataQuanta.writeTextFile(textFileSink.textFileUrl, s => s.toString)
//      case Some(objectFileSink: BlossomContext.ObjectFileSink) => dataQuanta.writeTextFile(objectFileSink.textFileUrl, s => s.toString)
//      case None => throw new WayangException("All contexts must be attached to an output sink.")
//    }
  }

  private def writeToTempFile(obj: AnyRef): Path = {
    val tempFile = Files.createTempFile("serialized", ".tmp")
    val fos = new FileOutputStream(tempFile.toFile)
    try {
      fos.write(SerializationUtils.serialize(obj))
    } finally {
      fos.close()
    }
    tempFile
  }

  private def readFromTempFile(path: Path): AnyRef = {
    val fis = new FileInputStream(path.toFile)
    try {
      SerializationUtils.deserialize(fis.readAllBytes())
    } finally {
      fis.close()
      Files.deleteIfExists(path)
    }
  }
}

