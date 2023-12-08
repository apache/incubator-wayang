package org.apache.wayang.api.implicits

import org.apache.wayang.api.{DataQuanta, PlanBuilder}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

object PlanBuilderImplicits {

  implicit class AsynchronousPlanBuilder[Out1: ClassTag, Out2: ClassTag](planBuilder: PlanBuilder) {
    def combineFromAsync[NewOut: ClassTag](result1: Future[DataQuantaAsyncResult[Out1]],
                                           result2: Future[DataQuantaAsyncResult[Out2]],
                                           combiner: (DataQuanta[Out1], DataQuanta[Out2]) => DataQuanta[NewOut]
                                             ): AsyncDataQuanta[NewOut] = {
      val combinedFuture = for {
        asyncResult1 <- result1
        asyncResult2 <- result2
      } yield {
        val dq1 = planBuilder.readObjectFile[Out1](asyncResult1.resultString)
        val dq2 = planBuilder.readObjectFile[Out2](asyncResult2.resultString)
        val combinedDataQuanta = combiner(dq1, dq2)
        combinedDataQuanta
      }
      new AsyncDataQuanta[NewOut](combinedFuture)
    }
  }

}
