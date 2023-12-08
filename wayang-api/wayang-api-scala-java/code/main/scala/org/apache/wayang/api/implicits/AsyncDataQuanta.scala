package org.apache.wayang.api.implicits

import org.apache.wayang.api.DataQuanta
import org.apache.wayang.api.implicits.DataQuantaImplicits._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.implicitConversions
import scala.reflect.ClassTag

class AsyncDataQuanta[Out: ClassTag](val futureDataQuanta: Future[DataQuanta[Out]]) {

  def map[NewOut: ClassTag](f: Out => NewOut): AsyncDataQuanta[NewOut] = {
    val newFuture = futureDataQuanta.map(dataQuanta => dataQuanta.map(f))(ec)
    new AsyncDataQuanta(newFuture)
  }

  def filter(p: Out => Boolean): AsyncDataQuanta[Out] = {
    val newFuture = futureDataQuanta.map(dataQuanta => dataQuanta.filter(p))(ec)
    new AsyncDataQuanta(newFuture)
  }

  def writeTextFile(url: String, formatterUdf: Out => String): Unit = {
    futureDataQuanta.map(dataQuanta => dataQuanta.writeTextFile(url, formatterUdf))
  }

  def runAsync(tempFileOut: String): Future[DataQuantaAsyncResult[Out]] = {
    futureDataQuanta.map { dataQuanta =>
      dataQuanta.runAsync(tempFileOut)
      DataQuantaAsyncResult(tempFileOut, implicitly[ClassTag[Out]])
    }
  }

  // TODO: Add other operators

}

