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

import org.apache.wayang.basic.operators.TextFileSink
import org.apache.wayang.core.api.exception.WayangException

import java.util.concurrent.Executors
import scala.concurrent.Future.sequence
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionException, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

class MultiContextDataQuanta[Out: ClassTag](val operators: List[DataQuanta[Out]], outputIndex: Int = 0)(val multiContextPlanBuilder: MultiContextPlanBuilder) {

  def map[NewOut: ClassTag](udf: Out => NewOut): MultiContextDataQuanta[NewOut] = {
    new MultiContextDataQuanta[NewOut](
      operators.map(operator => operator.map[NewOut](udf))
    )(multiContextPlanBuilder)
  }

  def filter(udf: Out => Boolean): MultiContextDataQuanta[Out] = {
    new MultiContextDataQuanta[Out](
      operators.map(operator => operator.filter(udf))
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

/*
  def execute(): Unit = {
    operators.zip(multiContextPlanBuilder.contexts).foreach {
      case (operator, context) =>
        context.sink match {
          case Some(textFileSink: BlossomContext.TextFileSink) => operator.writeTextFile(textFileSink.textFileUrl, s => s.toString)
          case Some(objectFileSink: BlossomContext.ObjectFileSink) => operator.writeTextFile(objectFileSink.textFileUrl, s => s.toString)
          case None => throw new WayangException("All contexts must be attached to an output sink.")
        }
    }
  }
  */

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

}
