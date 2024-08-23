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


package org.apache.wayang.api

import org.apache.wayang.api.async.DataQuantaImplicits._
import org.apache.wayang.api.async.PlanBuilderImplicits._
import org.apache.wayang.core.api.WayangContext
import org.apache.wayang.core.api.exception.WayangException
import org.apache.wayang.core.plan.wayangplan.Operator
import org.apache.wayang.core.platform.Platform

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag

class MultiContextDataQuanta[Out: ClassTag](private val dataQuantaMap: Map[Long, DataQuanta[Out]])(private val multiContextPlanBuilder: MultiContextPlanBuilder) {

  /**
   * Apply the specified function to each [[DataQuanta]].
   *
   * @tparam NewOut the type of elements in the resulting [[MultiContextDataQuanta]]
   * @param f the function to apply to each element
   * @return a new [[MultiContextDataQuanta]] with elements transformed by the function
   */
  def forEach[NewOut: ClassTag](f: DataQuanta[Out] => DataQuanta[NewOut]): MultiContextDataQuanta[NewOut] =
    new MultiContextDataQuanta[NewOut](dataQuantaMap.mapValues(f))(this.multiContextPlanBuilder)


  /**
   * Merges this and `that` [[MultiContextDataQuanta]], by using the `f` function to merge the corresponding [[DataQuanta]].
   * Returns a new [[MultiContextDataQuanta]] containing the results.
   *
   * @tparam ThatOut the type of data contained in `that` [[MultiContextDataQuanta]]
   * @tparam NewOut  the type of data contained in the resulting [[MultiContextDataQuanta]]
   * @param that the [[MultiContextDataQuanta]] to apply the function on
   * @param f    the function to apply on the corresponding data quanta
   * @return a new [[MultiContextDataQuanta]] containing the results of applying the function
   */
  def combineEach[ThatOut: ClassTag, NewOut: ClassTag](that: MultiContextDataQuanta[ThatOut],
                                                       f: (DataQuanta[Out], DataQuanta[ThatOut]) => DataQuanta[NewOut]): MultiContextDataQuanta[NewOut] =
    new MultiContextDataQuanta[NewOut](this.dataQuantaMap.map { case (key, thisDataQuanta) =>
      val thatDataQuanta = that.dataQuantaMap(key)
      key -> f(thisDataQuanta, thatDataQuanta)
    })(this.multiContextPlanBuilder)


  /**
   * Restrict the [[Operator]] of each [[MultiContext]] to run on certain [[Platform]]s.
   *
   * @param platforms on that the [[Operator]] may be executed
   * @return this instance
   */
  def withTargetPlatforms(platforms: Platform*): MultiContextDataQuanta[Out] = {
    new MultiContextDataQuanta[Out](this.dataQuantaMap.mapValues(_.withTargetPlatforms(platforms: _*)))(multiContextPlanBuilder)
  }


  /**
   * Restrict the [[Operator]] of specified [[MultiContext]] to run on certain [[Platform]]s.
   *
   * @param multiContext the [[MultiContext]] to restrict
   * @param platforms      on that the [[Operator]] may be executed
   * @return this instance
   */
  def withTargetPlatforms(multiContext: MultiContext, platforms: Platform*): MultiContextDataQuanta[Out] = {
    val updatedDataQuanta = dataQuantaMap(multiContext.id).withTargetPlatforms(platforms: _*)
    val updatedDataQuantaMap = dataQuantaMap.updated(multiContext.id, updatedDataQuanta)
    new MultiContextDataQuanta[Out](updatedDataQuantaMap)(this.multiContextPlanBuilder)
  }


  /**
   * Executes the plan asynchronously.
   *
   * @param timeout the maximum time to wait for the execution to complete.
   *                If not specified, the execution will block indefinitely.
   */
  def execute(timeout: Duration = Duration.Inf): Unit = {

    val asyncResults = multiContextPlanBuilder.multiContexts.map(multiContext => {

      // For each multiContext get its corresponding dataQuanta
      val dataQuanta = dataQuantaMap(multiContext.id)

      multiContext.getSink match {

        // Execute plan asynchronously
        case Some(textFileSink: MultiContext.TextFileSink) =>
          dataQuanta.writeTextFileAsync(textFileSink.url)

        // Execute plan asynchronously
        case Some(objectFileSink: MultiContext.ObjectFileSink) =>
          dataQuanta.writeObjectFileAsync(objectFileSink.url)

        case None =>
          throw new WayangException("All contexts must be attached to an output sink.")

        case _ =>
          throw new WayangException("Invalid sink.")
      }
    })

    // Block indefinitely until all futures finish
    val aggregateFuture: Future[List[Any]] = Future.sequence(asyncResults)
    Await.result(aggregateFuture, timeout)

  }


  /**
   * Merge the underlying [[DataQuanta]]s asynchronously using the [[DataQuanta.union]] operator.
   *
   * @param mergeContext The Wayang context for merging the [[DataQuanta]]s.
   * @param timeout      The maximum time to wait for all futures to finish. Default is [[Duration.Inf]] (indefinitely).
   * @return A [[DataQuanta]] representing the result of merging and unioning each of the [[DataQuanta]].
   * @throws WayangException if any of the contexts are not attached to a merge sink or if the sink is invalid.
   */
  def mergeUnion(mergeContext: WayangContext, timeout: Duration = Duration.Inf): DataQuanta[Out] = {

    // Execute plans asynchronously
    val asyncResults = multiContextPlanBuilder.multiContexts.map(multiContext => {

      // For each multiContext get its corresponding dataQuanta
      val dataQuanta = dataQuantaMap(multiContext.id)

      // Get the sink of the multiContext (it should be a merge sink)
      multiContext.getSink match {

        // And execute plan asynchronously
        case Some(mergeFileSink: MultiContext.MergeFileSink) =>
          dataQuanta.runAsync(mergeFileSink.url)

        case None =>
          throw new WayangException("All contexts must be attached to a merge sink.")

        case _ =>
          throw new WayangException("Invalid sink.")
      }
    })

    // Get futures
    val futures: List[Future[Any]] = asyncResults.map(_.future)

    // Block indefinitely until all futures finish
    val aggregateFuture: Future[List[Any]] = Future.sequence(futures)
    Await.result(aggregateFuture, timeout)

    // Create plan builder for the new merge context
    val planBuilder = new PlanBuilder(mergeContext).withUdfJarsOf(this.getClass)

    // Sources to merge
    var sources: List[DataQuanta[Out]] = List()

    // Create sources by loading each one using the merge context
    asyncResults.foreach(dataQuantaAsyncResult2 => sources = sources :+ planBuilder.loadAsync(dataQuantaAsyncResult2))

    // Merge sources with union and return
    sources.reduce((dq1, dq2) => dq1.union(dq2))
  }

}
