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

import org.apache.wayang.api.DataQuanta
import org.apache.wayang.api.implicits.DataQuantaImplicits._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.implicitConversions
import scala.reflect.ClassTag

class AsyncDataQuanta[Out: ClassTag](val futureDataQuanta: Future[DataQuanta[Out]]) {

  def map[NewOut: ClassTag](f: Out => NewOut): AsyncDataQuanta[NewOut] = {
    val newFuture = futureDataQuanta.map(dataQuanta => dataQuanta.map(f))
    new AsyncDataQuanta(newFuture)
  }

  def filter(p: Out => Boolean): AsyncDataQuanta[Out] = {
    val newFuture = futureDataQuanta.map(dataQuanta => dataQuanta.filter(p))
    new AsyncDataQuanta(newFuture)
  }

  def writeTextFile(url: String, formatterUdf: Out => String): Future[Unit] = {
    futureDataQuanta.map(dataQuanta => dataQuanta.writeTextFile(url, formatterUdf))
  }

  def runAsync(tempFileOut: String): Future[DataQuantaRunAsyncResult[Out]] = {
    futureDataQuanta.flatMap { dataQuanta =>
      dataQuanta.runAsync(tempFileOut).map { _ =>
        DataQuantaRunAsyncResult(tempFileOut, implicitly[ClassTag[Out]])
      }
    }
  }

  // TODO: Add other operators

}

