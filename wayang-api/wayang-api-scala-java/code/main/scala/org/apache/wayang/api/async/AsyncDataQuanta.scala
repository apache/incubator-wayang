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

import org.apache.wayang.api.DataQuanta

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.implicitConversions
import scala.reflect.ClassTag

class AsyncDataQuanta[Out: ClassTag](val futureDataQuanta: Future[DataQuanta[Out]]) {

  private[api] def andThenTransform[NewOut: ClassTag](plan: DataQuanta[Out] => DataQuanta[NewOut]): AsyncDataQuanta[NewOut] = {
    new AsyncDataQuanta[NewOut](futureDataQuanta.map(plan(_)))
  }

  private[api] def andThenRunAsync(tempFileOut: String): Future[DataQuantaAsyncResult[Out]] = {
    futureDataQuanta.flatMap(runAsyncWithTempFileOut(_, tempFileOut))
  }

  private[api] def andThenWriteTextFileOut(url: String): Future[Unit] = {
    futureDataQuanta.flatMap(runAsyncWithTextFileOut(_, url))
  }

  private[api] def andThenWriteObjectFileOut(url: String): Future[Unit] = {
    futureDataQuanta.flatMap(runAsyncWithObjectFileOut(_, url))
  }

}

