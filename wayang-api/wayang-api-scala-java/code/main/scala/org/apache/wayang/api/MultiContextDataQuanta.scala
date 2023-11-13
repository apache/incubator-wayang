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
import org.apache.wayang.basic.operators.{ObjectFileSink, TextFileSink}
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.api.exception.WayangException
import org.apache.wayang.core.plan.wayangplan.{Operator, WayangPlan}
import org.apache.wayang.core.util.ReflectionUtils
import org.apache.wayang.spark.Spark

import java.io.{FileInputStream, FileOutputStream}
import java.nio.file.{Files, Path}
import scala.reflect.ClassTag
import java.util.concurrent.{ConcurrentLinkedQueue, Executors}
import scala.collection.mutable.ListBuffer
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

    // ConcurrentLinkedQueue is thread-safe and suitable for storing paths of temp files.
    val tempFiles: ConcurrentLinkedQueue[Path] = new ConcurrentLinkedQueue[Path]()
    val processes: ListBuffer[Process] = ListBuffer() // To store the processes

    dataQuantaList.zip(multiContextPlanBuilder.contexts).foreach {
      case (dataQuanta, context) =>
        val operatorPath = MultiContextDataQuanta.writeToTempFile(dataQuanta.operator)
        val contextPath = MultiContextDataQuanta.writeToTempFile(context)

        // Store the paths for later deletion
        tempFiles.add(operatorPath)
        tempFiles.add(contextPath)

        val wayangHome = System.getenv("WAYANG_HOME")
        println(s"About to start a process with args ${(operatorPath, contextPath)}")
        val processBuilder = new ProcessBuilder(s"$wayangHome/bin/wayang-submit", "org.apache.wayang.api.MultiContextDataQuanta", operatorPath.toString, contextPath.toString)
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT)
        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT)

        val process = processBuilder.start()
        processes += process // Store the process for later
    }

    // Wait for all processes to complete
    processes.foreach(_.waitFor())

    // Delete all temporary files
    tempFiles.forEach(path => Files.deleteIfExists(path))

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

}


object MultiContextDataQuanta {
  def main(args: Array[String]): Unit = {
    println("New process here")
    println(args.mkString("Array(", ", ", ")"))
    println()

    if (args.length != 2) {
      System.err.println("Expected two arguments: paths to the serialized dataQuanta and context.")
      System.exit(1)
    }

    val operatorPath = Path.of(args(0))
    val contextPath = Path.of(args(1))

    val operator = MultiContextDataQuanta.readFromTempFile[Operator](operatorPath)
    val context = MultiContextDataQuanta.readFromTempFile[BlossomContext](contextPath)

    context.getSink match {
      case Some(textFileSink: BlossomContext.TextFileSink) =>
        val sink = new TextFileSink[AnyRef](textFileSink.textFileUrl, classOf[AnyRef])
        operator.connectTo(0, sink, 0)
        context.execute(new WayangPlan(sink), ReflectionUtils.getDeclaringJar(classOf[MultiContextDataQuanta[_]]))

      case Some(objectFileSink: BlossomContext.ObjectFileSink) =>
        val sink = new ObjectFileSink[AnyRef](objectFileSink.textFileUrl, classOf[AnyRef])
        operator.connectTo(0, sink, 0)
        context.execute(new WayangPlan(sink), ReflectionUtils.getDeclaringJar(classOf[MultiContextDataQuanta[_]]))

      case None =>
        throw new WayangException("All contexts must be attached to an output sink.")

      case _ =>
        throw new WayangException("All contexts must be attached to an output sink.")
    }
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

  private def readFromTempFile[T : ClassTag](path: Path): T = {
    val fis = new FileInputStream(path.toFile)
    try {
      SerializationUtils.deserialize[T](fis.readAllBytes())
    } finally {
      fis.close()
      Files.deleteIfExists(path)
    }
  }
}
