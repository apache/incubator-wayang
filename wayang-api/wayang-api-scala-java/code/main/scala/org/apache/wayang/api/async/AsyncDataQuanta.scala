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
import org.apache.wayang.api.async.DataQuantaImplicits._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.implicitConversions
import scala.reflect.ClassTag

class AsyncDataQuanta[Out: ClassTag](val futureDataQuanta: Future[DataQuanta[Out]]) {

/*  def map[NewOut: ClassTag](f: Out => NewOut): AsyncDataQuanta[NewOut] = {
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
      runAsyncWithTempFileOut(dataQuanta, tempFileOut).map { _ =>
        DataQuantaRunAsyncResult(tempFileOut, implicitly[ClassTag[Out]])
      }
    }
  }*/

  def transform[NewOut: ClassTag](plan: DataQuanta[Out] => DataQuanta[NewOut]): AsyncDataQuanta[NewOut] = {
    new AsyncDataQuanta[NewOut](futureDataQuanta.map(plan(_)))
  }

  def andThenRunAsync(tempFileOut: String): Future[DataQuantaAsyncResult[Out]] = {
    futureDataQuanta.flatMap(runAsyncWithTempFileOut(_, tempFileOut))
  }

  def andThenWriteTextFileOut(url: String): Future[Unit] = {
    futureDataQuanta.flatMap(runAsyncWithTextFileOut(_, url))
  }

  def andThenWriteObjectFileOut(url: String): Future[Unit] = {
    futureDataQuanta.flatMap(runAsyncWithObjectFileOut(_, url))
  }

  def andThenRunAsync[NewOut: ClassTag](plan: DataQuanta[Out] => DataQuanta[NewOut], tempFileOut: String): Future[DataQuantaAsyncResult[Out]] = {
    futureDataQuanta.flatMap(dataQuanta =>
      runAsyncWithTempFileOut(plan(dataQuanta), tempFileOut).map { _ =>
        DataQuantaAsyncResult(tempFileOut, implicitly[ClassTag[Out]])
      }
    )
  }

  def andThenRunAsyncWithTextFileOut[NewOut: ClassTag](plan: DataQuanta[Out] => DataQuanta[NewOut], textFileOut: String): Future[Unit] = {
    futureDataQuanta.flatMap(dataQuanta => runAsyncWithTextFileOut(plan(dataQuanta), textFileOut))
  }

  def andThenRunAsyncWithObjectFileOut[NewOut: ClassTag](plan: DataQuanta[Out] => DataQuanta[NewOut], objectFileOut: String): Future[Unit] = {
    futureDataQuanta.flatMap(dataQuanta => runAsyncWithObjectFileOut(plan(dataQuanta), objectFileOut))
  }

}

