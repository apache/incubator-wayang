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

import org.apache.wayang.api.async.DataQuantaImplicits.DataQuantaRunAsyncImplicits
import org.apache.wayang.core.api.WayangContext
import org.apache.wayang.core.api.exception.WayangException
import org.apache.wayang.core.platform.Platform

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag

class MultiContextDataQuanta[Out: ClassTag](val dataQuantaMap: Map[Long, DataQuanta[Out]])(val multiContextPlanBuilder: MultiContextPlanBuilder) {

  /**
   * Apply the specified function to each DataQuanta.
   *
   * @tparam NewOut the type of elements in the resulting MultiContextDataQuanta
   * @param f the function to apply to each element
   * @return a new MultiContextDataQuanta with elements transformed by the function
   */
  def foreach[NewOut: ClassTag](f: DataQuanta[Out] => DataQuanta[NewOut]): MultiContextDataQuanta[NewOut] =
    new MultiContextDataQuanta[NewOut](dataQuantaMap.mapValues(f))(this.multiContextPlanBuilder)


  /**
   * Merges this and `that` MultiContextDataQuanta, by using the `f` function to merge the corresponding DataQuanta.
   * Returns a new MultiContextDataQuanta containing the results.
   *
   * @tparam ThatOut the type of data contained in `that` MultiContextDataQuanta
   * @tparam NewOut  the type of data contained in the resulting MultiContextDataQuanta
   * @param that the MultiContextDataQuanta to apply the function on
   * @param f    the function to apply on the corresponding data quanta
   * @return a new MultiContextDataQuanta containing the results of applying the function
   */
  def combineEach[ThatOut: ClassTag, NewOut: ClassTag](that: MultiContextDataQuanta[ThatOut],
                                                       f: (DataQuanta[Out], DataQuanta[ThatOut]) => DataQuanta[NewOut]): MultiContextDataQuanta[NewOut] =
    new MultiContextDataQuanta[NewOut](this.dataQuantaMap.map { case (key, thisDataQuanta) =>
      val thatDataQuanta = that.dataQuantaMap(key)
      key -> f(thisDataQuanta, thatDataQuanta)
    })(this.multiContextPlanBuilder)

  def withTargetPlatforms(platforms: Platform*): MultiContextDataQuanta[Out] = {
    new MultiContextDataQuanta[Out](this.dataQuantaMap.mapValues(_.withTargetPlatforms(platforms: _*)))(multiContextPlanBuilder)
  }

  def withTargetPlatforms(blossomContext: BlossomContext, platforms: Platform*): MultiContextDataQuanta[Out] = {
    val updatedDataQuanta = dataQuantaMap(blossomContext.id).withTargetPlatforms(platforms: _*)
    val updatedDataQuantaMap = dataQuantaMap.updated(blossomContext.id, updatedDataQuanta)
    new MultiContextDataQuanta[Out](updatedDataQuantaMap)(this.multiContextPlanBuilder)
  }

  /*def map[NewOut: ClassTag](udf: Out => NewOut): MultiContextDataQuanta[NewOut] =
    foreach(_.map(udf))

  def mapPartitions[NewOut: ClassTag](udf: Iterable[Out] => Iterable[NewOut]): MultiContextDataQuanta[NewOut] =
    foreach(_.mapPartitions(udf))

  def filter(udf: Out => Boolean): MultiContextDataQuanta[Out] =
    foreach(_.filter(udf))

  def flatMap[NewOut: ClassTag](udf: Out => Iterable[NewOut]): MultiContextDataQuanta[NewOut] =
    foreach(_.flatMap(udf))

  def sample(sampleSize: Int,
             datasetSize: Long = SampleOperator.UNKNOWN_DATASET_SIZE,
             seed: Option[Long] = None,
             sampleMethod: SampleOperator.Methods = SampleOperator.Methods.ANY): MultiContextDataQuanta[Out] =
    foreach(_.sample(sampleSize, datasetSize, seed, sampleMethod))

  def sampleDynamic(sampleSizeFunction: Int => Int,
                    datasetSize: Long = SampleOperator.UNKNOWN_DATASET_SIZE,
                    seed: Option[Long] = None,
                    sampleMethod: SampleOperator.Methods = SampleOperator.Methods.ANY): MultiContextDataQuanta[Out] =
    foreach(_.sampleDynamic(sampleSizeFunction, datasetSize, seed, sampleMethod))

  def reduceByKey[Key: ClassTag](keyUdf: Out => Key,
                                 udf: (Out, Out) => Out): MultiContextDataQuanta[Out] =
    foreach(_.reduceByKey(keyUdf, udf))

  def groupByKey[Key: ClassTag](keyUdf: Out => Key): MultiContextDataQuanta[java.lang.Iterable[Out]] =
    foreach(_.groupByKey(keyUdf))

  def reduce(udf: (Out, Out) => Out): MultiContextDataQuanta[Out] =
    foreach(_.reduce(udf))

  def union(that: MultiContextDataQuanta[Out]): MultiContextDataQuanta[Out] =
    combineEach(that, (thisDataQuanta, thatDataQuanta: DataQuanta[Out]) => thisDataQuanta.union(thatDataQuanta))

  def intersect(that: MultiContextDataQuanta[Out]): MultiContextDataQuanta[Out] =
    combineEach(that, (thisDataQuanta, thatDataQuanta: DataQuanta[Out]) => thisDataQuanta.intersect(thatDataQuanta))

  import org.apache.wayang.basic.data.{Tuple2 => WayangTuple2}

  def join[ThatOut: ClassTag, Key: ClassTag](thisKeyUdf: Out => Key,
                                             that: MultiContextDataQuanta[ThatOut],
                                             thatKeyUdf: ThatOut => Key)
  : MultiContextDataQuanta[WayangTuple2[Out, ThatOut]] =
    combineEach(that, (thisDataQuanta, thatDataQuanta: DataQuanta[ThatOut]) => thisDataQuanta.join(thisKeyUdf, thatDataQuanta, thatKeyUdf))

  def coGroup[ThatOut: ClassTag, Key: ClassTag](thisKeyUdf: Out => Key,
                                                that: MultiContextDataQuanta[ThatOut],
                                                thatKeyUdf: ThatOut => Key)
  : MultiContextDataQuanta[WayangTuple2[java.lang.Iterable[Out], java.lang.Iterable[ThatOut]]] =
    combineEach(that, (thisDataQuanta, thatDataQuanta: DataQuanta[ThatOut]) => thisDataQuanta.coGroup(thisKeyUdf, thatDataQuanta, thatKeyUdf))

  def cartesian[ThatOut: ClassTag](that: MultiContextDataQuanta[ThatOut])
  : MultiContextDataQuanta[WayangTuple2[Out, ThatOut]] =
    combineEach(that, (thisDataQuanta, thatDataQuanta: DataQuanta[ThatOut]) => thisDataQuanta.cartesian(thatDataQuanta))

  def sort[Key: ClassTag](keyUdf: Out => Key): MultiContextDataQuanta[Out] =
    foreach(_.sort(keyUdf))

  def zipWithId: MultiContextDataQuanta[WayangTuple2[java.lang.Long, Out]] =
    foreach(_.zipWithId)

  def distinct: MultiContextDataQuanta[Out] =
    foreach(_.distinct)

  def count: MultiContextDataQuanta[java.lang.Long] =
    foreach(_.count)

  def doWhile[ConvOut: ClassTag](udf: Iterable[ConvOut] => Boolean,
                                 bodyBuilder: DataQuanta[Out] => (DataQuanta[Out], DataQuanta[ConvOut]),
                                 numExpectedIterations: Int = 20)
  : MultiContextDataQuanta[Out] =
    foreach(_.doWhile(udf, bodyBuilder, numExpectedIterations))

  def repeat(n: Int, bodyBuilder: DataQuanta[Out] => DataQuanta[Out]): MultiContextDataQuanta[Out] =
    foreach(_.repeat(n, bodyBuilder))*/


  /**
   * Execute the plans asynchronously.
   *
   * This method runs the plans defined in `multiContextPlanBuilder.blossomContexts` list
   * asynchronously. Each plan is executed in parallel, using the `dataQuanta.runAsync` method.
   *
   * After all plans are submitted for execution, this method blocks indefinitely using
   * `Future.sequence` and `Await.result` to wait for all the futures to finish.
   *
   * @throws java.util.concurrent.TimeoutException if any of the futures don't complete within `Duration.Inf`
   * @throws java.lang.InterruptedException        if the current thread is interrupted while waiting for the futures to complete
   */
  def execute(): Unit = {

    // Execute plans asynchronously
    val futures: List[Future[Unit]] = multiContextPlanBuilder.blossomContexts.map(blossomContext => {
      val dataQuanta = dataQuantaMap(blossomContext.id)
      dataQuanta.runAsync(blossomContext)
    })

    // Block indefinitely until all futures finish
    val aggregateFuture: Future[List[Unit]] = Future.sequence(futures)
    Await.result(aggregateFuture, Duration.Inf)

  }


  /**
   * Executes a multi-context job and reads the results from object files.
   *
   * @param mergeContext the WayangContext used for merging the contexts
   * @return a list of DataQuanta[Out] containing the merged results
   * @throws WayangException if any of the contexts does not have an output sink attached or if the sink is invalid
   */
  private def executeAndReadSources(mergeContext: WayangContext): List[DataQuanta[Out]] = {

    // Execute multi context job
    this.execute()

    // Create plan builder for the new merge context
    val planBuilder = new PlanBuilder(mergeContext).withUdfJarsOf(classOf[MultiContextDataQuanta[_]])

    // Sources to merge
    var sources: List[DataQuanta[Out]] = List()

    // For each context, read its output from object file
    multiContextPlanBuilder.blossomContexts.foreach(context =>
      context.getSink match {

        case Some(objectFileSink: BlossomContext.ObjectFileSink) =>
          sources = sources :+ planBuilder.readObjectFile[Out](objectFileSink.textFileUrl)

        case None =>
          throw new WayangException("All contexts must be attached to an output sink.")

        case _ =>
          throw new WayangException("Invalid sink.")
      }
    )

    // Return list of DataQuanta
    sources
  }


  /**
   * Merges all DataQuanta into a single one using the union operator.
   *
   * @param mergeContext The WayangContext used to execute and read the sources.
   * @tparam Out The type of the data in the data sources.
   * @return A DataQuanta object representing the merged ones.
   */
  def mergeUnion(mergeContext: WayangContext): DataQuanta[Out] = {
    val sources: List[DataQuanta[Out]] = executeAndReadSources(mergeContext)
    sources.reduce((dq1, dq2) => dq1.union(dq2))
  }

}
