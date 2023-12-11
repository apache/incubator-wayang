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

