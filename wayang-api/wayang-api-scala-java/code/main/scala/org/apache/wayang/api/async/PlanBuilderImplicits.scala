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
import scala.concurrent.Future
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
  implicit class PlanBuilderMergeAsyncImplicits[Out1: ClassTag, Out2: ClassTag](planBuilder: PlanBuilder) {

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
    def mergeAsync[NewOut: ClassTag](result1: Future[DataQuantaAsyncResult[Out1]],
                                     result2: Future[DataQuantaAsyncResult[Out2]],
                                     combiner: (DataQuanta[Out1], DataQuanta[Out2]) => DataQuanta[NewOut],
                                     tempFileOut: String
                                    ): Future[DataQuantaAsyncResult[NewOut]] = {
      readAndCombineDataQuanta(result1, result2, combiner)
        .flatMap(combinedDataQuanta => runAsyncWithTempFileOut(combinedDataQuanta, tempFileOut))
    }

    /**
     * Merge the results of two asynchronous data quanta operations and save the combined result to a text file.
     *
     * @param result1     The result of the first asynchronous data quanta operation.
     * @param result2     The result of the second asynchronous data quanta operation.
     * @param combiner    A function that combines the results of the two data quanta operations.
     *                    The function takes two DataQuanta instances, one from each operation, and returns a new DataQuanta instance.
     * @param textFileOut The path to the text file where the combined result will be saved.
     * @return A Future that completes when the combined result has been saved to the text file.
     */
    def mergeAsyncWithTextFileOut[NewOut: ClassTag](result1: Future[DataQuantaAsyncResult[Out1]],
                                                    result2: Future[DataQuantaAsyncResult[Out2]],
                                                    combiner: (DataQuanta[Out1], DataQuanta[Out2]) => DataQuanta[NewOut],
                                                    textFileOut: String
                                                   ): Future[Unit] = {
      readAndCombineDataQuanta(result1, result2, combiner)
        .flatMap(combinedDataQuanta => runAsyncWithTextFileOut(combinedDataQuanta, textFileOut))
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
    private[api] def combineFromAsync[NewOut: ClassTag](result1: Future[DataQuantaAsyncResult[Out1]],
                                                        result2: Future[DataQuantaAsyncResult[Out2]],
                                                        combiner: (DataQuanta[Out1], DataQuanta[Out2]) => DataQuanta[NewOut]
                                                       ): AsyncDataQuanta[NewOut] = {
      new AsyncDataQuanta[NewOut](readAndCombineDataQuanta(result1, result2, combiner))
    }

    private def readAndCombineDataQuanta[NewOut: ClassTag](result1: Future[DataQuantaAsyncResult[Out1]],
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
