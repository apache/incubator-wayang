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

import scala.concurrent.Future
import scala.reflect.ClassTag


object PlanBuilderImplicits {

  /**
   * An implicit class that adds additional functionality to the PlanBuilder class
   * for loading results of previous asynchronous operations.
   *
   * @param planBuilder The PlanBuilder instance to extend.
   * @tparam Out The type of data to be loaded.
   */
  implicit class PlanBuilderLoadAsyncImplicit[Out: ClassTag](planBuilder: PlanBuilder) {
    /**
     * Loads the data from the result of a previous asynchronous operation.
     *
     * @param asyncResult The asynchronous result object containing the temporary file path of the data to be loaded.
     * @return A DataQuanta representing the loaded data.
     */
    def loadAsync(asyncResult: DataQuantaAsyncResult[Out]): DataQuanta[Out] = {
      planBuilder.readObjectFile[Out](asyncResult.tempFileOut)
    }
  }

}
