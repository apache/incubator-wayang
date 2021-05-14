/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package org.apache.wayang.api.dataquanta

import de.hpi.isg.profiledb.store.model.Experiment
import org.apache.commons.lang3.Validate
import org.apache.wayang.api.{PlanBuilder, basicDataUnitType, dataSetType, toConsumer, toSerializableBinaryOperator, toSerializableFlatteningFunction, toSerializableFunction, toSerializablePartitionFunction, toSerializablePredicate}
import org.apache.wayang.basic.data.{Tuple2 => WayangTuple2}
import org.apache.wayang.basic.function.ProjectionDescriptor
import org.apache.wayang.basic.operators._
import org.apache.wayang.core.function.FunctionDescriptor.{SerializableBinaryOperator, SerializableFunction, SerializablePredicate}
import org.apache.wayang.core.function._
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator
import org.apache.wayang.core.plan.wayangplan._
import org.apache.wayang.core.platform.Platform
import org.apache.wayang.core.util.{Tuple => WayangTuple}

import java.lang.{Iterable => JavaIterable}
import java.util.function.{Consumer, IntUnaryOperator, Function => JavaFunction}
import java.util.{Collection => JavaCollection}
import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import scala.reflect._

/**
  * Represents an intermediate result/data flow edge in a [[WayangPlan]].
  * However this is just a template that help to be easy extendable the API
  *
  * @param operator    a unary [[Operator]] that produces this instance
  * @param ev$1        the data type of the elements in this instance
  * @param planBuilder keeps track of the [[WayangPlan]] being build
  */
abstract class DataQuanta[Out: ClassTag](val operator: ElementaryOperator, outputIndex: Int = 0)(implicit val planBuilder: PlanBuilder) {

  Validate.isTrue(operator.getNumOutputs > outputIndex)

  /**
    * This instance corresponds to an [[OutputSlot]] of a wrapped [[Operator]].
    *
    * @return the said [[OutputSlot]]
    */
  implicit def output = operator.getOutput(outputIndex).asInstanceOf[OutputSlot[Out]]

  /**
    * Feed this instance into a [[MapOperator]].
    *
    * @param udf     UDF for the [[MapOperator]]
    * @param udfLoad optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the [[MapOperator]]'s output
    */
  def map[NewOut: ClassTag](udf: Out => NewOut,
                            udfLoad: LoadProfileEstimator = null):  DataQuanta[NewOut] = {
    mapJava(toSerializableFunction(udf), udfLoad)
  }

  /**
    * Feed this instance into a [[MapOperator]].
    *
    * @param udf     a Java 8 lambda expression as UDF for the [[MapOperator]]
    * @param udfLoad optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the [[MapOperator]]'s output
    */
  def mapJava[NewOut: ClassTag](udf: SerializableFunction[Out, NewOut],
                                udfLoad: LoadProfileEstimator = null): DataQuanta[NewOut]

  /**
    * Feed this instance into a [[MapPartitionsOperator]].
    *
    * @param udf         UDF for the [[MapPartitionsOperator]]
    * @param selectivity selectivity of the UDF
    * @param udfLoad     optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the [[MapPartitionsOperator]]'s output
    */
  def mapPartitions[NewOut: ClassTag](udf: Iterable[Out] => Iterable[NewOut],
                                      selectivity: ProbabilisticDoubleInterval = null,
                                      udfLoad: LoadProfileEstimator = null): DataQuanta[NewOut] = {
    mapPartitionsJava(toSerializablePartitionFunction(udf), selectivity, udfLoad)
  }

  /**
    * Feed this instance into a [[MapPartitionsOperator]].
    *
    * @param udf         a Java 8 lambda expression as UDF for the [[MapPartitionsOperator]]
    * @param selectivity selectivity of the UDF
    * @param udfLoad     optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the [[MapOperator]]'s output
    */
  def mapPartitionsJava[NewOut: ClassTag](udf: SerializableFunction[JavaIterable[Out], JavaIterable[NewOut]],
                                          selectivity: ProbabilisticDoubleInterval = null,
                                          udfLoad: LoadProfileEstimator = null): DataQuanta[NewOut]

  /**
    * Feed this instance into a [[MapOperator]] with a [[ProjectionDescriptor]].
    *
    * @param fieldNames names of the fields to be projected
    * @return a new instance representing the [[MapOperator]]'s output
    */
  def project[NewOut: ClassTag](fieldNames: Seq[String]): DataQuanta[NewOut]

  /**
    * Connects the [[operator]] to a further [[Operator]].
    *
    * @param operator   the [[Operator]] to connect to
    * @param inputIndex the input index of the [[Operator]]s [[InputSlot]]
    */
  private[api] def connectTo(operator: Operator, inputIndex: Int): Unit =
    this.operator.connectTo(outputIndex, operator, inputIndex)


  /**
    * Feed this instance into a [[FilterOperator]].
    *
    * @param udf         UDF for the [[FilterOperator]]
    * @param sqlUdf      UDF as SQL `WHERE` clause
    * @param udfLoad     optional [[LoadProfileEstimator]] for the `udf`
    * @param selectivity selectivity of the UDF
    * @return a new instance representing the [[FilterOperator]]'s output
    */
  def filter(udf: Out => Boolean,
             sqlUdf: String = null,
             selectivity: ProbabilisticDoubleInterval = null,
             udfLoad: LoadProfileEstimator = null) = {
    filterJava(toSerializablePredicate(udf), sqlUdf, selectivity, udfLoad)
  }

  /**
    * Feed this instance into a [[FilterOperator]].
    *
    * @param udf         UDF for the [[FilterOperator]]
    * @param sqlUdf      UDF as SQL `WHERE` clause
    * @param selectivity selectivity of the UDF
    * @param udfLoad     optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the [[FilterOperator]]'s output
    */
  def filterJava(udf: SerializablePredicate[Out],
                 sqlUdf: String = null,
                 selectivity: ProbabilisticDoubleInterval = null,
                 udfLoad: LoadProfileEstimator = null): DataQuanta[Out]

  /**
    * Feed this instance into a [[FlatMapOperator]].
    *
    * @param udf         UDF for the [[FlatMapOperator]]
    * @param selectivity selectivity of the UDF
    * @param udfLoad     optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the [[FlatMapOperator]]'s output
    */
  def flatMap[NewOut: ClassTag](udf: Out => Iterable[NewOut],
                                selectivity: ProbabilisticDoubleInterval = null,
                                udfLoad: LoadProfileEstimator = null): DataQuanta[NewOut] = {
    flatMapJava(toSerializableFlatteningFunction(udf), selectivity, udfLoad)
  }

  /**
    * Feed this instance into a [[FlatMapOperator]].
    *
    * @param udf         a Java 8 lambda expression as UDF for the [[FlatMapOperator]]
    * @param selectivity selectivity of the UDF
    * @param udfLoad     optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the [[FlatMapOperator]]'s output
    */
  def flatMapJava[NewOut: ClassTag](udf: SerializableFunction[Out, JavaIterable[NewOut]],
                                    selectivity: ProbabilisticDoubleInterval = null,
                                    udfLoad: LoadProfileEstimator = null): DataQuanta[NewOut]

  /**
    * Feed this instance into a [[SampleOperator]]. If this operation is inside of a loop, the sampling size
    * can be adjusted in each iteration.
    *
    * @param sampleSize   absolute size of the sample
    * @param datasetSize  optional size of the dataset to be sampled
    * @param sampleMethod the [[SampleOperator.Methods]] to use for sampling
    * @return a new instance representing the [[FlatMapOperator]]'s output
    */
  def sample(sampleSize: Int,
             datasetSize: Long = SampleOperator.UNKNOWN_DATASET_SIZE,
             seed: Option[Long] = None,
             sampleMethod: SampleOperator.Methods = SampleOperator.Methods.ANY): DataQuanta[Out] =
    sampleDynamic(_ => sampleSize, datasetSize, seed, sampleMethod)

  /**
    * Feed this instance into a [[SampleOperator]]. If this operation is inside of a loop, the sampling size
    * can be adjusted in each iteration.
    *
    * @param sampleSizeFunction absolute size of the sample as a function of the current iteration number
    * @param datasetSize        optional size of the dataset to be sampled
    * @param sampleMethod       the [[SampleOperator.Methods]] to use for sampling
    * @return a new instance representing the [[FlatMapOperator]]'s output
    */
  def sampleDynamic(sampleSizeFunction: Int => Int,
                    datasetSize: Long = SampleOperator.UNKNOWN_DATASET_SIZE,
                    seed: Option[Long] = None,
                    sampleMethod: SampleOperator.Methods = SampleOperator.Methods.ANY): DataQuanta[Out] = {
    sampleDynamicJava(
      new IntUnaryOperator {
        override def applyAsInt(operand: Int): Int = sampleSizeFunction(operand)
      },
      datasetSize,
      seed,
      sampleMethod
    )
  }


  /**
    * Feed this instance into a [[SampleOperator]].
    *
    * @param sampleSizeFunction absolute size of the sample as a function of the current iteration number
    * @param datasetSize        optional size of the dataset to be sampled
    * @param sampleMethod       the [[SampleOperator.Methods]] to use for sampling
    * @return a new instance representing the [[FlatMapOperator]]'s output
    */
  def sampleDynamicJava(sampleSizeFunction: IntUnaryOperator,
                        datasetSize: Long = SampleOperator.UNKNOWN_DATASET_SIZE,
                        seed: Option[Long] = None,
                        sampleMethod: SampleOperator.Methods = SampleOperator.Methods.ANY): DataQuanta[Out]

  /**
    * Assigns this instance a key extractor, which enables some key-based operations.
    *
    * @see KeyedDataQuanta
    * @param keyExtractor extracts the key from the [[DataQuanta]]
    * @return the [[KeyedDataQuanta]]
    */
  //TODO: may need to be build by the extenders
  def keyBy[Key: ClassTag](keyExtractor: Out => Key) : KeyedDataQuanta[Out, Key] = {
    keyByJava(keyExtractor)
  }

  /**
    * Assigns this instance a key extractor, which enables some key-based operations.
    *
    * @see KeyedDataQuanta
    * @param keyExtractor extracts the key from the [[DataQuanta]]
    * @return the [[KeyedDataQuanta]]
    */
  //TODO: may need to be build by the extenders
  def keyByJava[Key: ClassTag](keyExtractor: SerializableFunction[Out, Key]) : KeyedDataQuanta[Out, Key]

  /**
    * Feed this instance into a [[ReduceByOperator]].
    *
    * @param keyUdf  UDF to extract the grouping key from the data quanta
    * @param udf     aggregation UDF for the [[ReduceByOperator]]
    * @param udfLoad optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the [[ReduceByOperator]]'s output
    */
  def reduceByKey[Key: ClassTag](keyUdf: Out => Key,
                                 udf: (Out, Out) => Out,
                                 udfLoad: LoadProfileEstimator = null): DataQuanta[Out] = {
    reduceByKeyJava(toSerializableFunction(keyUdf), toSerializableBinaryOperator(udf), udfLoad)
  }

  /**
    * Feed this instance into a [[ReduceByOperator]].
    *
    * @param keyUdf  UDF to extract the grouping key from the data quanta
    * @param udf     aggregation UDF for the [[ReduceByOperator]]
    * @param udfLoad optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the [[ReduceByOperator]]'s output
    */
  def reduceByKeyJava[Key: ClassTag](keyUdf: SerializableFunction[Out, Key],
                                     udf: SerializableBinaryOperator[Out],
                                     udfLoad: LoadProfileEstimator = null) : DataQuanta[Out]

  /**
    * Feed this instance into a [[MaterializedGroupByOperator]].
    *
    * @param keyUdf     UDF to extract the grouping key from the data quanta
    * @param keyUdfLoad optional [[LoadProfileEstimator]] for the `keyUdf`
    * @return a new instance representing the [[MaterializedGroupByOperator]]'s output
    */
  def groupByKey[Key: ClassTag](keyUdf: Out => Key,
                                keyUdfLoad: LoadProfileEstimator = null): DataQuanta[java.lang.Iterable[Out]] =
    groupByKeyJava(toSerializableFunction(keyUdf), keyUdfLoad)

  /**
    * Feed this instance into a [[MaterializedGroupByOperator]].
    *
    * @param keyUdf     UDF to extract the grouping key from the data quanta
    * @param keyUdfLoad optional [[LoadProfileEstimator]] for the `keyUdf`
    * @return a new instance representing the [[MaterializedGroupByOperator]]'s output
    */
  def groupByKeyJava[Key: ClassTag](keyUdf: SerializableFunction[Out, Key],
                                    keyUdfLoad: LoadProfileEstimator = null): DataQuanta[java.lang.Iterable[Out]]

  /**
    * Feed this instance into a [[GlobalReduceOperator]].
    *
    * @param udf     aggregation UDF for the [[GlobalReduceOperator]]
    * @param udfLoad optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the [[GlobalReduceOperator]]'s output
    */
  def reduce(udf: (Out, Out) => Out,
             udfLoad: LoadProfileEstimator = null): DataQuanta[Out] =
    reduceJava(toSerializableBinaryOperator(udf), udfLoad)

  /**
    * Feed this instance into a [[GlobalReduceOperator]].
    *
    * @param udf     aggregation UDF for the [[GlobalReduceOperator]]
    * @param udfLoad optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the [[GlobalReduceOperator]]'s output
    */
  def reduceJava(udf: SerializableBinaryOperator[Out],
                 udfLoad: LoadProfileEstimator = null): DataQuanta[Out]

  /**
    * Feed this instance into a [[GlobalMaterializedGroupOperator]].
    *
    * @return a new instance representing the [[GlobalMaterializedGroupOperator]]'s output
    */
  def group(): DataQuanta[java.lang.Iterable[Out]]

  /**
    * Feed this instance and a further instance into a [[UnionAllOperator]].
    *
    * @param that the other instance to union with
    * @return a new instance representing the [[UnionAllOperator]]'s output
    */
  def union(that: DataQuanta[Out]): DataQuanta[Out]

  /**
    * Feed this instance and a further instance into a [[IntersectOperator]].
    *
    * @param that the other instance to intersect with
    * @return a new instance representing the [[IntersectOperator]]'s output
    */
  def intersect(that: DataQuanta[Out]): DataQuanta[Out]

  /**
    * Feeds this and a further instance into a [[JoinOperator]].
    *
    * @param thisKeyUdf UDF to extract keys from data quanta in this instance
    * @param that       the other instance
    * @param thatKeyUdf UDF to extract keys from data quanta from `that` instance
    * @return a new instance representing the [[JoinOperator]]'s output
    */
  def join[ThatOut: ClassTag, Key: ClassTag] (thisKeyUdf: Out => Key,
                                              that: DataQuanta[ThatOut],
                                              thatKeyUdf: ThatOut => Key): DataQuanta[WayangTuple2[Out, ThatOut]] = {
    joinJava(toSerializableFunction(thisKeyUdf), that, toSerializableFunction(thatKeyUdf))
  }

  /**
    * Feeds this and a further instance into a [[JoinOperator]].
    *
    * @param thisKeyUdf UDF to extract keys from data quanta in this instance
    * @param that       the other instance
    * @param thatKeyUdf UDF to extract keys from data quanta from `that` instance
    * @return a new instance representing the [[JoinOperator]]'s output
    */
  def joinJava[ThatOut: ClassTag, Key: ClassTag] (thisKeyUdf: SerializableFunction[Out, Key],
                                                  that: DataQuanta[ThatOut],
                                                  thatKeyUdf: SerializableFunction[ThatOut, Key]) : DataQuanta[WayangTuple2[Out, ThatOut]]

  /**
    * Feeds this and a further instance into a [[CoGroupOperator]].
    *
    * @param thisKeyUdf UDF to extract keys from data quanta in this instance
    * @param that       the other instance
    * @param thatKeyUdf UDF to extract keys from data quanta from `that` instance
    * @return a new instance representing the [[CoGroupOperator]]'s output
    */
  def coGroup[ThatOut: ClassTag, Key: ClassTag] (thisKeyUdf: Out => Key,
                                                 that: DataQuanta[ThatOut],
                                                  thatKeyUdf: ThatOut => Key): DataQuanta[WayangTuple2[java.lang.Iterable[Out], java.lang.Iterable[ThatOut]]] = {
    coGroupJava(toSerializableFunction(thisKeyUdf), that, toSerializableFunction(thatKeyUdf))
  }

  /**
    * Feeds this and a further instance into a [[CoGroupOperator]].
    *
    * @param thisKeyUdf UDF to extract keys from data quanta in this instance
    * @param that       the other instance
    * @param thatKeyUdf UDF to extract keys from data quanta from `that` instance
    * @return a new instance representing the [[CoGroupOperator]]'s output
    */
  def coGroupJava[ThatOut: ClassTag, Key: ClassTag] (thisKeyUdf: SerializableFunction[Out, Key],
                                                     that: DataQuanta[ThatOut],
                                                     thatKeyUdf: SerializableFunction[ThatOut, Key]): DataQuanta[WayangTuple2[java.lang.Iterable[Out], java.lang.Iterable[ThatOut]]]

  /**
    * Feeds this and a further instance into a [[SortOperator]].
    *
    * @param keyUdf UDF to extract key from data quanta in this instance
    * @return a new instance representing the [[SortOperator]]'s output
    */
  def sort[Key: ClassTag] (keyUdf: Out => Key): DataQuanta[Out] = {
    sortJava(toSerializableFunction(keyUdf))
  }

  /**
    * Feeds this and a further instance into a [[SortOperator]].
    *
    * @param keyUdf UDF to extract key from data quanta in this instance
    * @return a new instance representing the [[SortOperator]]'s output
    */
  def sortJava[Key: ClassTag] (keyUdf: SerializableFunction[Out, Key]) : DataQuanta[Out]

  /**
    * Feeds this and a further instance into a [[CartesianOperator]].
    *
    * @param that the other instance
    * @return a new instance representing the [[CartesianOperator]]'s output
    */
  def cartesian[ThatOut: ClassTag](that: DataQuanta[ThatOut]) : DataQuanta[WayangTuple2[Out, ThatOut]]

  /**
    * Feeds this instance into a [[ZipWithIdOperator]].
    *
    * @return a new instance representing the [[ZipWithIdOperator]]'s output
    */
  def zipWithId: DataQuanta[WayangTuple2[java.lang.Long, Out]]

  /**
    * Feeds this instance into a [[DistinctOperator]].
    *
    * @return a new instance representing the [[DistinctOperator]]'s output
    */
  def distinct: DataQuanta[Out]

  /**
    * Feeds this instance into a [[CountOperator]].
    *
    * @return a new instance representing the [[CountOperator]]'s output
    */
  def count: DataQuanta[java.lang.Long]

  /**
    * Feeds this instance into a do-while loop (guarded by a [[DoWhileOperator]].
    *
    * @param udf         condition to be evaluated after each iteration
    * @param bodyBuilder creates the loop body
    * @param udfLoad     optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the final output of the [[DoWhileOperator]]
    */
  def doWhile[ConvOut: ClassTag](udf: Iterable[ConvOut] => Boolean,
                                 bodyBuilder: DataQuanta[Out] => (DataQuanta[Out], DataQuanta[ConvOut]),
                                 numExpectedIterations: Int = 20,
                                 udfLoad: LoadProfileEstimator = null) =
    doWhileJava(
      toSerializablePredicate((in: JavaCollection[ConvOut]) => udf(JavaConversions.collectionAsScalaIterable(in))),
      new JavaFunction[DataQuanta[Out], WayangTuple[DataQuanta[Out], DataQuanta[ConvOut]]] {
        override def apply(t: DataQuanta[Out]) = {
          val result = bodyBuilder(t)
          new WayangTuple(result._1, result._2)
        }
      },
      numExpectedIterations, udfLoad
    )

  /**
    * Feeds this instance into a do-while loop (guarded by a [[DoWhileOperator]].
    *
    * @param udf         condition to be evaluated after each iteration
    * @param bodyBuilder creates the loop body
    * @param udfLoad     optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the final output of the [[DoWhileOperator]]
    */
  def doWhileJava[ConvOut: ClassTag](
                                      udf: SerializablePredicate[JavaCollection[ConvOut]],
                                      bodyBuilder: JavaFunction[DataQuanta[Out], WayangTuple[DataQuanta[Out], DataQuanta[ConvOut]]],
                                      numExpectedIterations: Int = 20,
                                      udfLoad: LoadProfileEstimator = null) = {
    // Create the DoWhileOperator.
    val doWhileOperator = new DoWhileOperator(
      dataSetType[Out],
      dataSetType[ConvOut],
      new PredicateDescriptor(udf, basicDataUnitType[JavaCollection[ConvOut]], null, udfLoad),
      numExpectedIterations
    )
    this.connectTo(doWhileOperator, DoWhileOperator.INITIAL_INPUT_INDEX)

    // Create and wire the loop body.
    val loopDataQuanta = DataQuantaFactory.build[Out](doWhileOperator, DoWhileOperator.ITERATION_OUTPUT_INDEX)
    val iterationResults = bodyBuilder.apply(loopDataQuanta)
    iterationResults.getField0.connectTo(doWhileOperator, DoWhileOperator.ITERATION_INPUT_INDEX)
    iterationResults.getField1.connectTo(doWhileOperator, DoWhileOperator.CONVERGENCE_INPUT_INDEX)

    // Return the iteration result.
    DataQuantaFactory.build[Out](doWhileOperator, DoWhileOperator.FINAL_OUTPUT_INDEX)
  }

  /**
    * Feeds this instance into a for-loop (guarded by a [[LoopOperator]].
    *
    * @param n           number of iterations
    * @param bodyBuilder creates the loop body
    * @return a new instance representing the final output of the [[LoopOperator]]
    */
  def repeat(n: Int, bodyBuilder: DataQuanta[Out] => DataQuanta[Out]) =
    repeatJava(n,
      new JavaFunction[DataQuanta[Out], DataQuanta[Out]] {
        override def apply(t: DataQuanta[Out]) = bodyBuilder(t)
      }
    )

  /**
    * Feeds this instance into a for-loop (guarded by a [[LoopOperator]].
    *
    * @param n           number of iterations
    * @param bodyBuilder creates the loop body
    * @return a new instance representing the final output of the [[LoopOperator]]
    */
  def repeatJava(n: Int, bodyBuilder: JavaFunction[DataQuanta[Out], DataQuanta[Out]]) = {
    // Create the RepeatOperator.
    val repeatOperator = new RepeatOperator(n, dataSetType[Out])
    this.connectTo(repeatOperator, RepeatOperator.INITIAL_INPUT_INDEX)

    // Create and wire the loop body.
    val loopDataQuanta = DataQuantaFactory.build[Out](repeatOperator, RepeatOperator.ITERATION_OUTPUT_INDEX)
    val iterationResult = bodyBuilder.apply(loopDataQuanta)
    iterationResult.connectTo(repeatOperator, RepeatOperator.ITERATION_INPUT_INDEX)

    // Return the iteration result.
    DataQuantaFactory.build[Out](repeatOperator, RepeatOperator.FINAL_OUTPUT_INDEX)
  }

  /**
    * Use a custom [[Operator]]. Note that only [[Operator]]s with a single [[InputSlot]] and [[OutputSlot]] are allowed.
    * Otherwise, use [[PlanBuilder.customOperator()]].
    *
    * @param operator the custom [[Operator]]
    * @tparam T the output type of the `operator`
    * @return a new instance representing the output of the custom [[Operator]]
    */
  def customOperator[T](operator: Operator): DataQuanta[T] = {
    Validate.isTrue(
      operator.getNumInputs == 1 && operator.getNumOutputs == 1,
      "customOperator() accepts only operators with a single input and output. Use PlanBuilder.customOperator(...)."
    )
    planBuilder.customOperator(operator, this)(0).asInstanceOf[DataQuanta[T]]
  }

  /**
    * Use a broadcast in the [[Operator]] that creates this instance.
    *
    * @param sender        provides the broadcast data quanta
    * @param broadcastName the name with that the broadcast will be registered
    * @return this instance
    */
  def withBroadcast(sender: DataQuanta[_], broadcastName: String) = {
    require(this.planBuilder eq sender.planBuilder, s"$this and $sender must use the same plan builders.")
    sender.broadcast(this, broadcastName)
    this
  }

  /**
    * Broadcasts the data quanta in this instance to a further instance.
    *
    * @param receiver      the instance that receives the broadcast
    * @param broadcastName the name with that the broadcast will be registered
    */
  private def broadcast(receiver: DataQuanta[_], broadcastName: String) =
    receiver.registerBroadcast(this.operator, this.outputIndex, broadcastName)

  /**
    * Register a further instance as broadcast.
    *
    * @param sender        provides the broadcast data quanta
    * @param outputIndex   identifies the output index of the sender
    * @param broadcastName the name with that the broadcast will be registered
    */
  private def registerBroadcast(sender: Operator, outputIndex: Int, broadcastName: String) =
    sender.broadcastTo(outputIndex, this.operator, broadcastName)


  /**
    * Perform a local action on each data quantum in this instance. Triggers execution.
    *
    * @param f the action to perform
    */
  def foreach(f: Out => _): Unit = foreachJava(toConsumer(f))

  /**
    * Perform a local action on each data quantum in this instance. Triggers execution.
    *
    * @param f the action to perform as Java 8 lambda expression
    */
  //TODO validate if it is the correct way
  def foreachJava(f: Consumer[Out]): Unit = {
    val sink = new LocalCallbackSink(f, dataSetType[Out])
    sink.setName("foreach()")
    this.connectTo(sink, 0)
    this.planBuilder.sinks += sink
    this.planBuilder.buildAndExecute()
    this.planBuilder.sinks.clear()
  }

  /**
    * Collect the data quanta in this instance. Triggers execution.
    *
    * @return the data quanta
    */
  //TODO validate if it is the correct way
  def collect(): Iterable[Out] = {
    // Set up the sink.
    val collector = new java.util.LinkedList[Out]()
    val sink = LocalCallbackSink.createCollectingSink(collector, dataSetType[Out])
    sink.setName("collect()")
    this.connectTo(sink, 0)

    // Do the execution.
    this.planBuilder.sinks += sink
    this.planBuilder.buildAndExecute()
    this.planBuilder.sinks.clear()

    // Return the collected values.
    collector
  }

  /**
    * Write the data quanta in this instance to a text file. Triggers execution.
    *
    * @param url          URL to the text file
    * @param formatterUdf UDF to format data quanta to [[String]]s
    * @param udfLoad      optional [[LoadProfileEstimator]] for the `udf`
    */
  def writeTextFile(url: String,
                    formatterUdf: Out => String,
                    udfLoad: LoadProfileEstimator = null): Unit = {
    writeTextFileJava(url, toSerializableFunction(formatterUdf), udfLoad)
  }

  /**
    * Write the data quanta in this instance to a text file. Triggers execution.
    *
    * @param url          URL to the text file
    * @param formatterUdf UDF to format data quanta to [[String]]s
    * @param udfLoad      optional [[LoadProfileEstimator]] for the `udf`
    */
  //TODO validate if it is the correct way
  def writeTextFileJava(url: String,
                        formatterUdf: SerializableFunction[Out, String],
                        udfLoad: LoadProfileEstimator = null): Unit = {
    val sink = new TextFileSink[Out](
      url,
      new TransformationDescriptor(formatterUdf, basicDataUnitType[Out], basicDataUnitType[String], udfLoad)
    )
    sink.setName(s"Write to $url")
    this.connectTo(sink, 0)

    // Do the execution.
    this.planBuilder.sinks += sink
    this.planBuilder.buildAndExecute()
    this.planBuilder.sinks.clear()
  }

  /**
    * Restrict the producing [[Operator]] to run on certain [[Platform]]s.
    *
    * @param platforms on that the [[Operator]] may be executed
    * @return this instance
    */
  def withTargetPlatforms(platforms: Platform*) = {
    platforms.foreach(this.operator.addTargetPlatform)
    this
  }

  /**
    * Set a name for the [[Operator]] that creates this instance.
    *
    * @param name the name
    * @return this instance
    */
  def withName(name: String) = {
    this.operator.setName(name)
    this
  }

  /**
    * Sets a [[CardinalityEstimator]] for the [[Operator]] that creates this instance.
    *
    * @param estimator that should be set
    * @return this instance
    */
  def withCardinalityEstimator(estimator: CardinalityEstimator) = {
    this.operator.setCardinalityEstimator(outputIndex, estimator)
    this
  }

  /**
    * Defines user-code JAR files that might be needed to transfer to execution platforms.
    *
    * @param paths paths to JAR files that should be transferred
    * @return this instance
    */
  def withUdfJars(paths: String*) = {
    this.planBuilder withUdfJars (paths: _*)
    this
  }


  /**
    * Defines the [[Experiment]] that should collects metrics of the [[WayangPlan]].
    *
    * @param experiment the [[Experiment]]
    * @return this instance
    */
  def withExperiment(experiment: Experiment) = {
    this.planBuilder withExperiment experiment
    this
  }

  /**
    * Defines user-code JAR files that might be needed to transfer to execution platforms.
    *
    * @param classes whose JAR files should be transferred
    * @return this instance
    */
  def withUdfJarsOf(classes: Class[_]*) = {
    this.planBuilder withUdfJarsOf (classes: _*)
    this
  }

  override def toString = s"DataQuanta[$output]"

}

object DataQuanta {

  def create[T](output: OutputSlot[T])(implicit planBuilder: PlanBuilder): DataQuanta[_] = {
    DataQuantaFactory.build[T](output.getOwner.asInstanceOf[ElementaryOperator], output.getIndex)(ClassTag(output.getType.getDataUnitType.getTypeClass), planBuilder)
  }

}
