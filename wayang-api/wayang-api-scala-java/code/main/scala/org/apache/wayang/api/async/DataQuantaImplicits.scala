package org.apache.wayang.api.async

import org.apache.wayang.api
import org.apache.wayang.api.{BlossomContext, DataQuanta, async}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

object DataQuantaImplicits {

  implicit class DataQuantaRunAsyncImplicits[T: ClassTag](dataQuanta: DataQuanta[T]) {

    def runAsync(blossomContext: BlossomContext): Future[Unit] = {
      async.runAsyncBody(dataQuanta, blossomContext)
    }

    def runAsync2(tempFileOut: String, dataQuantaAsyncResults: DataQuantaAsyncResult2[_]*): DataQuantaAsyncResult2[T] = {
      // Schedule new future to run after the previous futures have completed
      val resultFuture: Future[_] = getFutureSequence(dataQuantaAsyncResults: _*).flatMap { _ =>
        async.runAsyncWithTempFileOut(dataQuanta, tempFileOut)
      }
      DataQuantaAsyncResult2(tempFileOut, implicitly[ClassTag[T]], resultFuture)
    }

    def writeTextFileAsync(url: String, dataQuantaAsyncResults: DataQuantaAsyncResult2[_]*): Future[Unit] = {
      // Schedule new future to run after the previous futures have completed
      getFutureSequence(dataQuantaAsyncResults: _*).flatMap { _ =>
        async.runAsyncWithTextFileOut(dataQuanta, url)
      }
    }

    def writeObjectFileAsync(url: String, dataQuantaAsyncResults: DataQuantaAsyncResult2[_]*): Future[Unit] = {
      // Schedule new future to run after the previous futures have completed
      getFutureSequence(dataQuantaAsyncResults: _*).flatMap { _ =>
        async.runAsyncWithObjectFileOut(dataQuanta, url)
      }
    }

    // Extract method refactoring
    private def getFutureSequence(dataQuantaAsyncResult2: DataQuantaAsyncResult2[_]*): Future[Seq[Any]] = {
      // Extract the futures
      val futures: Seq[Future[Any]] = dataQuantaAsyncResult2.map(_.future)

      // Create a Future of Seq[Any]
      Future.sequence(futures)
    }
  }

}