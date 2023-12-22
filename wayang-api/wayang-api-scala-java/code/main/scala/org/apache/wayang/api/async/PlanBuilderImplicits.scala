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

import org.apache.wayang.api.{DataQuanta, PlanBuilder, async}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, Future, TimeoutException}
import scala.reflect.ClassTag


object PlanBuilderImplicits {


  /**
   * Implicit class that adds an asynchronous execution method to a PlanBuilder.
   * This class provides a convenient way to execute a plan asynchronously and write the result to a temporary file.
   *
   * @param planBuilder the PlanBuilder instance to extend with asynchronous execution method.
   * @tparam Out the output type of the plan.
   */
  implicit class PlanBuilderRunAsyncImplicit[Out: ClassTag](planBuilder: PlanBuilder) {
    /**
     * Executes a plan asynchronously and writes the result to a temporary file.
     *
     * @param plan        the function that builds the plan to execute.
     * @param tempFileOut the file path where the result will be written to.
     * @tparam Out the output type of the plan.
     * @return a Future that will contain the result of the execution.
     */
    def runAsync(plan: PlanBuilder => DataQuanta[Out], tempFileOut: String): Future[DataQuantaAsyncResult[Out]] = {
      async.runAsyncWithTempFileOut(plan(planBuilder), tempFileOut)
    }
  }


  /**
   * Implicit class that provides merge functionality to PlanBuilder.
   *
   * @param planBuilder The PlanBuilder instance to extend with merge functionality.
   * @tparam Out1 The type of the first data quanta.
   * @tparam Out2 The type of the second data quanta.
   */
  implicit class PlanBuilderMergeAsyncImplicits(planBuilder: PlanBuilder) {

    /**
     * Merges the results of two asynchronous operations, using a combiner function to create a new result.
     *
     * @param result1     The future of the first asynchronous operation result.
     * @param result2     The future of the second asynchronous operation result.
     * @param combiner    The function to combine the results of the two asynchronous operations.
     * @param tempFileOut The temporary file output path for the combined result.
     * @tparam NewOut The type of the new result.
     * @return A Future of `DataQuantaAsyncResult[NewOut]` representing the combined result.
     */
    def mergeAsync[Out1: ClassTag, Out2: ClassTag, NewOut: ClassTag](result1: Future[DataQuantaAsyncResult[Out1]],
                                                                     result2: Future[DataQuantaAsyncResult[Out2]],
                                                                     combiner: (DataQuanta[Out1], DataQuanta[Out2]) => DataQuanta[NewOut],
                                                                     tempFileOut: String
                                                                    ): Future[DataQuantaAsyncResult[NewOut]] = {
      readAndCombineDataQuanta(result1, result2, combiner)
        .flatMap(combinedDataQuanta => runAsyncWithTempFileOut(combinedDataQuanta, tempFileOut))
    }



    /**
     * Merges the results of two asynchronous operations using a combiner function and writes
     * the merged result to a text file. The function will throw a TimeoutException if the
     * operation takes more than the provided timeout.
     *
     * @param result1 the `Future[DataQuantaAsyncResult[Out1]]` that is the result of the first operation
     * @param result2 the `Future[DataQuantaAsyncResult[Out2]]` that is the result of the second operation
     * @param combiner the function that takes two DataQuanta (one of type [Out1] and another of type [Out2])
     *                 and returns a DataQuanta of type [NewOut]
     * @param textFileOut the path to the text file where the combined result will be written
     * @param timeoutInSeconds the maximum time to wait for the merge and write operation to
     *                         complete before timing out. Default value is 60 seconds.
     * @tparam NewOut the generic type parameter for the new output data quanta.
     *
     * @throws TimeoutException if the operation does not complete within the specified timeout duration
     */
    def mergeAsyncWithTextFileOut[Out1: ClassTag, Out2: ClassTag, NewOut: ClassTag](result1: Future[DataQuantaAsyncResult[Out1]],
                                                                                    result2: Future[DataQuantaAsyncResult[Out2]],
                                                                                    combiner: (DataQuanta[Out1], DataQuanta[Out2]) => DataQuanta[NewOut],
                                                                                    textFileOut: String,
                                                                                    timeoutInSeconds: Long = 60L
                                                                                   ): Unit = {
      val future = readAndCombineDataQuanta(result1, result2, combiner)
        .flatMap(combinedDataQuanta => runAsyncWithTextFileOut(combinedDataQuanta, textFileOut))
      Await.result(future, Duration(timeoutInSeconds, SECONDS))
    }


    /**
     * Merges the results of two asynchronous operations using a combiner function and writes the merged result to an object file.
     * The function will throw a TimeoutException if the operation takes more than the provided timeout.
     *
     * @param result1 the `Future[DataQuantaAsyncResult[Out1]]` that is the result of the first operation
     * @param result2 the `Future[DataQuantaAsyncResult[Out2]]` that is the result of the second operation
     * @param combiner the function that takes two DataQuanta, one of type [Out1] and another of type [Out2], and returns a DataQuanta of type [NewOut]
     * @param objectFileOut the location of the output file where the merged DataQuanta will be written
     * @param timeoutInSeconds the maximum time to wait for the merge and write operation to complete before timing out. Default value is 60 seconds.
     * @tparam NewOut the generic type parameter for the new output data quanta.
     *
     * @throws TimeoutException if the operation does not complete within the specified timeout duration
     */
    def mergeAsyncWithObjectFileOut[Out1: ClassTag, Out2: ClassTag, NewOut: ClassTag](result1: Future[DataQuantaAsyncResult[Out1]],
                                                                                      result2: Future[DataQuantaAsyncResult[Out2]],
                                                                                      combiner: (DataQuanta[Out1], DataQuanta[Out2]) => DataQuanta[NewOut],
                                                                                      objectFileOut: String,
                                                                                      timeoutInSeconds: Long = 60L
                                                                                     ): Unit = {
      val future = readAndCombineDataQuanta(result1, result2, combiner)
        .flatMap(combinedDataQuanta => runAsyncWithObjectFileOut(combinedDataQuanta, objectFileOut))
      Await.result(future, Duration(timeoutInSeconds, SECONDS))
    }


    /**
     * The difference with the above methods is that this one is for internal use only, thus the `private[api]` modifier.
     *
     * Combines the results of two asynchronous DataQuanta operations using a combiner function.
     *
     * @param result1  The result of the first asynchronous operation.
     * @param result2  The result of the second asynchronous operation.
     * @param combiner A function that takes two DataQuanta instances of type Out1 and Out2, and returns a DataQuanta instance of type NewOut.
     * @tparam NewOut The type of the resulting DataQuanta.
     * @return An AsyncDataQuanta instance representing the combined result.
     */
    private[api] def combineFromAsync[Out1: ClassTag, Out2: ClassTag, NewOut: ClassTag](result1: Future[DataQuantaAsyncResult[Out1]],
                                                                                        result2: Future[DataQuantaAsyncResult[Out2]],
                                                                                        combiner: (DataQuanta[Out1], DataQuanta[Out2]) => DataQuanta[NewOut]
                                                                                       ): AsyncDataQuanta[NewOut] = {
      new AsyncDataQuanta[NewOut](readAndCombineDataQuanta(result1, result2, combiner))
    }


    /**
     * Helper method to combine the results of two asynchronous data quanta operations using the provided combiner function.
     *
     * @param result1  The future result of the first data quanta operation.
     * @param result2  The future result of the second data quanta operation.
     * @param combiner The function used to combine the results of the two data quantas.
     *                 Takes two data quantas of types Out1 and Out2, and returns a new data quanta of type NewOut.
     * @tparam NewOut The type parameter for the output data quanta.
     * @return A future that will contain the combined data quanta result.
     */
    private def readAndCombineDataQuanta[Out1: ClassTag, Out2: ClassTag, NewOut: ClassTag](result1: Future[DataQuantaAsyncResult[Out1]],
                                                                                           result2: Future[DataQuantaAsyncResult[Out2]],
                                                                                           combiner: (DataQuanta[Out1], DataQuanta[Out2]) => DataQuanta[NewOut]
                                                                                          ): Future[DataQuanta[NewOut]] = {
      for {
        asyncResult1 <- result1
        asyncResult2 <- result2
        dq1 = planBuilder.readObjectFile[Out1](asyncResult1.tempFileOut)
        dq2 = planBuilder.readObjectFile[Out2](asyncResult2.tempFileOut)
      } yield {
        combiner(dq1, dq2)
      }
    }

  }

}
