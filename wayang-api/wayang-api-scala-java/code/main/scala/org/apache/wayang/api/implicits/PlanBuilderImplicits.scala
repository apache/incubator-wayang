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

import org.apache.wayang.api.{DataQuanta, PlanBuilder}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

object PlanBuilderImplicits {

  implicit class PlanBuilderCombineFromAsyncImplicit[Out1: ClassTag, Out2: ClassTag](planBuilder: PlanBuilder) {
    def combineFromAsync[NewOut: ClassTag](result1: Future[DataQuantaRunAsyncResult[Out1]],
                                           result2: Future[DataQuantaRunAsyncResult[Out2]],
                                           combiner: (DataQuanta[Out1], DataQuanta[Out2]) => DataQuanta[NewOut]
                                             ): AsyncDataQuanta[NewOut] = {
      val combinedFuture = for {
        asyncResult1 <- result1
        asyncResult2 <- result2
      } yield {
        val dq1 = planBuilder.readObjectFile[Out1](asyncResult1.tempFileOut)
        val dq2 = planBuilder.readObjectFile[Out2](asyncResult2.tempFileOut)
        val combinedDataQuanta = combiner(dq1, dq2)
        combinedDataQuanta
      }
      new AsyncDataQuanta[NewOut](combinedFuture)
    }
  }

}
