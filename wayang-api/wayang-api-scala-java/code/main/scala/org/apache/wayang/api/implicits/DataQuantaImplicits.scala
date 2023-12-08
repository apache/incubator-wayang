package org.apache.wayang.api.implicits

import org.apache.wayang.api.DataQuanta

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag


object DataQuantaImplicits {

  implicit class AsynchronousDataQuanta[Out: ClassTag](dataQuanta: DataQuanta[Out])(implicit ct: ClassTag[Out]) {
    def runAsync(tempFileOut: String): Future[DataQuantaAsyncResult[Out]] = Future {
      println(s"Running asynchronously with param: $tempFileOut")
      DataQuantaAsyncResult("Hello", ct)
    }
  }

}