package org.apache.wayang.api.async

import org.apache.wayang.api.{BlossomContext, DataQuanta, async}

import scala.concurrent.Future
import scala.reflect.ClassTag

object DataQuantaImplicits {

  implicit class DataQuantaRunAsyncImplicits[T: ClassTag](dataQuanta: DataQuanta[T]) {
    def runAsync(blossomContext: BlossomContext): Future[Unit] = {
      async.runAsyncBody(dataQuanta, blossomContext)
    }
  }

}