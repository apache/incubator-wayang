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

import org.apache.wayang.api
import org.apache.wayang.api.{MultiContext, DataQuanta, async}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * Implicit conversions and utility methods for asynchronous operations on `DataQuanta`.
 */
object DataQuantaImplicits {

  implicit class DataQuantaRunAsyncImplicits[T: ClassTag](dataQuanta: DataQuanta[T]) {

    /**
     * Asynchronously runs a Wayang plan and writes out to `tempFileOut`.
     * This method schedules a new future to run after the previous futures in `dataQuantaAsyncResults` have completed.
     * The result of the execution is represented by an instance of `DataQuantaAsyncResult`.
     *
     * @param tempFileOut            the temporary file output path
     * @param dataQuantaAsyncResults a variable number of previous asynchronous results
     * @return a future representing the asynchronous result of running the data quanta
     */
    def runAsync(tempFileOut: String, dataQuantaAsyncResults: DataQuantaAsyncResult[_]*): DataQuantaAsyncResult[T] = {
      // Schedule new future to run after the previous futures in `dataQuantaAsyncResults` have completed
      val resultFuture: Future[_] = getFutureSequence(dataQuantaAsyncResults: _*).flatMap { _ =>
        async.runAsyncWithTempFileOut(dataQuanta, tempFileOut)
      }
      DataQuantaAsyncResult(tempFileOut, implicitly[ClassTag[T]], resultFuture)
    }

    /**
     * Similar to [[DataQuanta.writeTextFile]] but for asynchronous execution.
     *
     * @param url                    The URL where the text file should be written.
     * @param dataQuantaAsyncResults The asynchronous results of data quanta operations.
     *                               Variable arguments of type 'DataQuantaAsyncResult[_]'.
     *                               Multiple results can be provided in any order.
     *                               The method will wait for all the provided futures to complete
     *                               before executing the file writing operation.
     * @return A future unit indicating the completion of the text file writing operation.
     * @throws RuntimeException if any of the provided asynchronous results
     *                          fails with an exception during execution.
     */
    def writeTextFileAsync(url: String, dataQuantaAsyncResults: DataQuantaAsyncResult[_]*): Future[Unit] = {
      // Schedule new future to run after the previous futures in `dataQuantaAsyncResults` have completed
      getFutureSequence(dataQuantaAsyncResults: _*).flatMap { _ =>
        async.runAsyncWithTextFileOut(dataQuanta, url)
      }
    }

    /**
     * Similar to [[DataQuanta.writeObjectFile]] but for asynchronous execution.
     *
     * @param url                    The URL of the object file sink.
     * @param dataQuantaAsyncResults The asynchronous results of data quanta operations.
     *                               Variable arguments of type 'DataQuantaAsyncResult[_]'.
     *                               Multiple results can be provided in any order.
     *                               The method will wait for all the provided futures to complete
     *                               before executing the file writing operation.* @return A `Future` that completes when the data quanta have been written successfully.
     */
    def writeObjectFileAsync(url: String, dataQuantaAsyncResults: DataQuantaAsyncResult[_]*): Future[Unit] = {
      // Schedule new future to run after the previous futures in `dataQuantaAsyncResults` have completed
      getFutureSequence(dataQuantaAsyncResults: _*).flatMap { _ =>
        async.runAsyncWithObjectFileOut(dataQuanta, url)
      }
    }

    // Extract futures
    private def getFutureSequence(dataQuantaAsyncResult2: DataQuantaAsyncResult[_]*): Future[Seq[Any]] = {
      // Extract the futures
      val futures: Seq[Future[Any]] = dataQuantaAsyncResult2.map(_.future)

      // Create a Future of Seq[Any]
      Future.sequence(futures)
    }
  }

}