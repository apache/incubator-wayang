package org.qcri.rheem.api

import _root_.java.util.function.{Consumer, Function => JavaFunction}
import _root_.java.util.{Collection => JavaCollection}

import org.apache.commons.lang3.Validate
import org.qcri.rheem.basic.operators._
import org.qcri.rheem.core.function.FunctionDescriptor.{SerializableBinaryOperator, SerializableFunction}
import org.qcri.rheem.core.function.PredicateDescriptor.SerializablePredicate
import org.qcri.rheem.core.function.{FlatMapDescriptor, PredicateDescriptor, ReduceDescriptor, TransformationDescriptor}
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator
import org.qcri.rheem.core.optimizer.costs.LoadEstimator
import org.qcri.rheem.core.plan.rheemplan._
import org.qcri.rheem.core.platform.Platform
import org.qcri.rheem.core.util.{ReflectionUtils, RheemCollections, Tuple => RheemTuple}

import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import scala.reflect._

/**
  * Represents an intermediate result/data flow edge in a [[RheemPlan]].
  *
  * @param operator    a unary [[Operator]] that produces this instance
  * @param ev$1        the data type of the elements in this instance
  * @param planBuilder keeps track of the [[RheemPlan]] being build
  */
class DataQuanta[Out: ClassTag](val operator: ElementaryOperator, outputIndex: Int = 0)(implicit planBuilder: PlanBuilder) {

  Validate.isTrue(operator.getNumOutputs > outputIndex)

  /**
    * This instance corresponds to an [[OutputSlot]] of a wrapped [[Operator]].
    *
    * @return the said [[OutputSlot]]
    */
  implicit def output = operator.getOutput(outputIndex)

  /**
    * Feed this instance into a [[MapOperator]].
    *
    * @param udf        UDF for the [[MapOperator]]
    * @param udfCpuLoad optional [[LoadEstimator]] for the CPU consumption of the `udf`
    * @param udfRamLoad optional [[LoadEstimator]] for the RAM consumption of the `udf`
    * @return a new instance representing the [[MapOperator]]'s output
    */
  def map[NewOut: ClassTag](udf: Out => NewOut,
                            udfCpuLoad: LoadEstimator = null,
                            udfRamLoad: LoadEstimator = null): DataQuanta[NewOut] =
    mapJava(toSerializableFunction(udf), udfCpuLoad, udfRamLoad)

  /**
    * Feed this instance into a [[MapOperator]].
    *
    * @param udf        a Java 8 lambda expression as UDF for the [[MapOperator]]
    * @param udfCpuLoad optional [[LoadEstimator]] for the CPU consumption of the `udf`
    * @param udfRamLoad optional [[LoadEstimator]] for the RAM consumption of the `udf`
    * @return a new instance representing the [[MapOperator]]'s output
    */
  def mapJava[NewOut: ClassTag](udf: SerializableFunction[Out, NewOut],
                                udfCpuLoad: LoadEstimator = null,
                                udfRamLoad: LoadEstimator = null): DataQuanta[NewOut] = {
    val mapOperator = new MapOperator(new TransformationDescriptor(
      udf, basicDataUnitType[Out], basicDataUnitType[NewOut], udfCpuLoad, udfRamLoad
    ))
    this.connectTo(mapOperator, 0)
    mapOperator
  }

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
    * @param udfCpuLoad  optional [[LoadEstimator]] for the CPU consumption of the `udf`
    * @param udfRamLoad  optional [[LoadEstimator]] for the RAM consumption of the `udf`
    * @param selectivity selectivity of the UDF
    * @return a new instance representing the [[FilterOperator]]'s output
    */
  def filter(udf: Out => Boolean,
             selectivity: ProbabilisticDoubleInterval = null,
             udfCpuLoad: LoadEstimator = null,
             udfRamLoad: LoadEstimator = null) =
    filterJava(toSerializablePredicate(udf), selectivity, udfCpuLoad, udfRamLoad)

  /**
    * Feed this instance into a [[FilterOperator]].
    *
    * @param udf         UDF for the [[FilterOperator]]
    * @param selectivity selectivity of the UDF
    * @param udfCpuLoad  optional [[LoadEstimator]] for the CPU consumption of the `udf`
    * @param udfRamLoad  optional [[LoadEstimator]] for the RAM consumption of the `udf`
    * @return a new instance representing the [[FilterOperator]]'s output
    */
  def filterJava(udf: SerializablePredicate[Out],
                 selectivity: ProbabilisticDoubleInterval = null,
                 udfCpuLoad: LoadEstimator = null,
                 udfRamLoad: LoadEstimator = null): DataQuanta[Out] = {
    val filterOperator = new FilterOperator(new PredicateDescriptor(udf, basicDataUnitType[Out], selectivity, udfCpuLoad, udfRamLoad))
    this.connectTo(filterOperator, 0)
    filterOperator
  }

  /**
    * Feed this instance into a [[FlatMapOperator]].
    *
    * @param udf         UDF for the [[FlatMapOperator]]
    * @param selectivity selectivity of the UDF
    * @param udfCpuLoad  optional [[LoadEstimator]] for the CPU consumption of the `udf`
    * @param udfRamLoad  optional [[LoadEstimator]] for the RAM consumption of the `udf`
    * @return a new instance representing the [[FlatMapOperator]]'s output
    */
  def flatMap[NewOut: ClassTag](udf: Out => Iterable[NewOut],
                                selectivity: ProbabilisticDoubleInterval = null,
                                udfCpuLoad: LoadEstimator = null,
                                udfRamLoad: LoadEstimator = null): DataQuanta[NewOut] =
    flatMapJava(toSerializableFlatteningFunction(udf), selectivity, udfCpuLoad, udfRamLoad)

  /**
    * Feed this instance into a [[FlatMapOperator]].
    *
    * @param udf         a Java 8 lambda expression as UDF for the [[FlatMapOperator]]
    * @param selectivity selectivity of the UDF
    * @param udfCpuLoad  optional [[LoadEstimator]] for the CPU consumption of the `udf`
    * @param udfRamLoad  optional [[LoadEstimator]] for the RAM consumption of the `udf`
    * @return a new instance representing the [[FlatMapOperator]]'s output
    */
  def flatMapJava[NewOut: ClassTag](udf: SerializableFunction[Out, java.lang.Iterable[NewOut]],
                                    selectivity: ProbabilisticDoubleInterval = null,
                                    udfCpuLoad: LoadEstimator = null,
                                    udfRamLoad: LoadEstimator = null): DataQuanta[NewOut] = {
    val flatMapOperator = new FlatMapOperator(new FlatMapDescriptor(
      udf, basicDataUnitType[Out], basicDataUnitType[NewOut], selectivity, udfCpuLoad, udfRamLoad
    ))
    this.connectTo(flatMapOperator, 0)
    flatMapOperator
  }

  /**
    * Feed this instance into a [[ReduceByOperator]].
    *
    * @param keyUdf UDF to extract the grouping key from the data quanta
    * @param udf    aggregation UDF for the [[ReduceByOperator]]
    * @param udfCpuLoad  optional [[LoadEstimator]] for the CPU consumption of the `udf`
    * @param udfRamLoad  optional [[LoadEstimator]] for the RAM consumption of the `udf`
    * @return a new instance representing the [[ReduceByOperator]]'s output
    */
  def reduceByKey[Key: ClassTag](keyUdf: Out => Key,
                                 udf: (Out, Out) => Out,
                                 udfCpuLoad: LoadEstimator = null,
                                 udfRamLoad: LoadEstimator = null): DataQuanta[Out] =
    reduceByKeyJava(toSerializableFunction(keyUdf), toSerializableBinaryOperator(udf), udfCpuLoad, udfRamLoad)

  /**
    * Feed this instance into a [[ReduceByOperator]].
    *
    * @param keyUdf UDF to extract the grouping key from the data quanta
    * @param udf    aggregation UDF for the [[ReduceByOperator]]
    * @param udfCpuLoad  optional [[LoadEstimator]] for the CPU consumption of the `udf`
    * @param udfRamLoad  optional [[LoadEstimator]] for the RAM consumption of the `udf`
    * @return a new instance representing the [[ReduceByOperator]]'s output
    */
  def reduceByKeyJava[Key: ClassTag](keyUdf: SerializableFunction[Out, Key],
                                     udf: SerializableBinaryOperator[Out],
                                     udfCpuLoad: LoadEstimator = null,
                                     udfRamLoad: LoadEstimator = null)
  : DataQuanta[Out] = {
    val reduceByOperator = new ReduceByOperator(
      new TransformationDescriptor(keyUdf, basicDataUnitType[Out], basicDataUnitType[Key]),
      new ReduceDescriptor(udf, groupedDataUnitType[Out], basicDataUnitType[Out], udfCpuLoad, udfRamLoad)
    )
    this.connectTo(reduceByOperator, 0)
    reduceByOperator
  }

  /**
    * Feed this instance into a [[MaterializedGroupByOperator]].
    *
    * @param keyUdf UDF to extract the grouping key from the data quanta
    * @param udfCpuLoad  optional [[LoadEstimator]] for the CPU consumption of the `udf`
    * @param udfRamLoad  optional [[LoadEstimator]] for the RAM consumption of the `udf`
    * @return a new instance representing the [[MaterializedGroupByOperator]]'s output
    */
  def groupByKey[Key: ClassTag](keyUdf: Out => Key,
                                udfCpuLoad: LoadEstimator = null,
                                udfRamLoad: LoadEstimator = null): DataQuanta[java.lang.Iterable[Out]] = {
    val groupByOperator = new MaterializedGroupByOperator(
      new TransformationDescriptor(toSerializableFunction(keyUdf), basicDataUnitType[Out], basicDataUnitType[Key]),
      dataSetType[Out],
      groupedDataSetType[Out]
    )
    this.connectTo(groupByOperator, 0)
    groupByOperator
  }

  /**
    * Feed this instance into a [[GlobalReduceOperator]].
    *
    * @param udf aggregation UDF for the [[GlobalReduceOperator]]
    * @param udfCpuLoad  optional [[LoadEstimator]] for the CPU consumption of the `udf`
    * @param udfRamLoad  optional [[LoadEstimator]] for the RAM consumption of the `udf`
    * @return a new instance representing the [[GlobalReduceOperator]]'s output
    */
  def reduce(udf: (Out, Out) => Out,
             udfCpuLoad: LoadEstimator = null,
             udfRamLoad: LoadEstimator = null): DataQuanta[Out] =
    reduceJava(toSerializableBinaryOperator(udf), udfCpuLoad, udfRamLoad)

  /**
    * Feed this instance into a [[GlobalReduceOperator]].
    *
    * @param udf aggregation UDF for the [[GlobalReduceOperator]]
    * @param udfCpuLoad  optional [[LoadEstimator]] for the CPU consumption of the `udf`
    * @param udfRamLoad  optional [[LoadEstimator]] for the RAM consumption of the `udf`
    * @return a new instance representing the [[GlobalReduceOperator]]'s output
    */
  def reduceJava(udf: SerializableBinaryOperator[Out],
                 udfCpuLoad: LoadEstimator = null,
                 udfRamLoad: LoadEstimator = null): DataQuanta[Out] = {
    val globalReduceOperator = new GlobalReduceOperator(
      new ReduceDescriptor(udf, groupedDataUnitType[Out], basicDataUnitType[Out], udfCpuLoad, udfRamLoad)
    )
    this.connectTo(globalReduceOperator, 0)
    globalReduceOperator
  }

  /**
    * Feed this instance into a [[GlobalMaterializedGroupOperator]].
    *
    * @return a new instance representing the [[GlobalMaterializedGroupOperator]]'s output
    */
  def group(): DataQuanta[java.lang.Iterable[Out]] = {
    val groupOperator = new GlobalMaterializedGroupOperator(dataSetType[Out], groupedDataSetType[Out])
    this.connectTo(groupOperator, 0)
    groupOperator
  }

  /**
    * Feed this instance and a further instance into a [[UnionAllOperator]].
    *
    * @param that the other instance to union with
    * @return a new instance representing the [[UnionAllOperator]]'s output
    */
  def union(that: DataQuanta[Out]): DataQuanta[Out] = {
    val unionAllOperator = new UnionAllOperator(dataSetType[Out])
    this.connectTo(unionAllOperator, 0)
    that.connectTo(unionAllOperator, 1)
    unionAllOperator
  }

  /**
    * Feed this instance and a further instance into a [[IntersectOperator]].
    *
    * @param that the other instance to intersect with
    * @return a new instance representing the [[IntersectOperator]]'s output
    */
  def intersect(that: DataQuanta[Out]): DataQuanta[Out] = {
    val intersectOperator = new IntersectOperator(dataSetType[Out])
    this.connectTo(intersectOperator, 0)
    that.connectTo(intersectOperator, 1)
    intersectOperator
  }

  /**
    * Feeds this and a further instance into a [[JoinOperator]].
    *
    * @param thisKeyUdf UDF to extract keys from data quanta in this instance
    * @param that       the other instance
    * @param thatKeyUdf UDF to extract keys from data quanta from `that` instance
    * @return a new instance representing the [[JoinOperator]]'s output
    */
  def join[ThatOut: ClassTag, Key: ClassTag]
  (thisKeyUdf: Out => Key,
   that: DataQuanta[ThatOut],
   thatKeyUdf: ThatOut => Key)
  : DataQuanta[org.qcri.rheem.basic.data.Tuple2[Out, ThatOut]] =
    joinJava(toSerializableFunction(thisKeyUdf), that, toSerializableFunction(thatKeyUdf))

  /**
    * Feeds this and a further instance into a [[JoinOperator]].
    *
    * @param thisKeyUdf UDF to extract keys from data quanta in this instance
    * @param that       the other instance
    * @param thatKeyUdf UDF to extract keys from data quanta from `that` instance
    * @return a new instance representing the [[JoinOperator]]'s output
    */
  def joinJava[ThatOut: ClassTag, Key: ClassTag]
  (thisKeyUdf: SerializableFunction[Out, Key],
   that: DataQuanta[ThatOut],
   thatKeyUdf: SerializableFunction[ThatOut, Key])
  : DataQuanta[org.qcri.rheem.basic.data.Tuple2[Out, ThatOut]] = {
    val joinOperator = new JoinOperator(
      new TransformationDescriptor(thisKeyUdf, basicDataUnitType[Out], basicDataUnitType[Key]),
      new TransformationDescriptor(thatKeyUdf, basicDataUnitType[ThatOut], basicDataUnitType[Key])
    )
    this.connectTo(joinOperator, 0)
    that.connectTo(joinOperator, 1)
    joinOperator
  }

  /**
    * Feeds this and a further instance into a [[CartesianOperator]].
    *
    * @param that the other instance
    * @return a new instance representing the [[CartesianOperator]]'s output
    */
  def cartesian[ThatOut: ClassTag](that: DataQuanta[ThatOut])
  : DataQuanta[org.qcri.rheem.basic.data.Tuple2[Out, ThatOut]] = {
    val cartesianOperator = new CartesianOperator(dataSetType[Out], dataSetType[ThatOut])
    this.connectTo(cartesianOperator, 0)
    that.connectTo(cartesianOperator, 1)
    cartesianOperator
  }

  /**
    * Feeds this instance into a [[ZipWithIdOperator]].
    *
    * @return a new instance representing the [[ZipWithIdOperator]]'s output
    */
  def zipWithId: DataQuanta[org.qcri.rheem.basic.data.Tuple2[Long, Out]] = {
    val zipWithIdOperator = new ZipWithIdOperator(dataSetType[Out])
    this.connectTo(zipWithIdOperator, 0)
    zipWithIdOperator
  }

  /**
    * Feeds this instance into a [[DistinctOperator]].
    *
    * @return a new instance representing the [[DistinctOperator]]'s output
    */
  def distinct: DataQuanta[Out] = {
    val distinctOperator = new DistinctOperator(dataSetType[Out])
    this.connectTo(distinctOperator, 0)
    distinctOperator
  }

  /**
    * Feeds this instance into a [[CountOperator]].
    *
    * @return a new instance representing the [[CountOperator]]'s output
    */
  def count: DataQuanta[Long] = {
    val countOperator = new CountOperator(dataSetType[Out])
    this.connectTo(countOperator, 0)
    countOperator
  }


  /**
    * Feeds this instance into a do-while loop (guarded by a [[DoWhileOperator]].
    *
    * @param udf         condition to be evaluated after each iteration
    * @param bodyBuilder creates the loop body
    * @param udfCpuLoad  optional [[LoadEstimator]] for the CPU consumption of the `udf`
    * @param udfRamLoad  optional [[LoadEstimator]] for the RAM consumption of the `udf`
    * @return a new instance representing the final output of the [[DoWhileOperator]]
    */
  def doWhile[ConvOut: ClassTag](udf: Iterable[ConvOut] => Boolean,
                                 bodyBuilder: DataQuanta[Out] => (DataQuanta[Out], DataQuanta[ConvOut]),
                                 numExpectedIterations: Int = 20,
                                 udfCpuLoad: LoadEstimator = null,
                                 udfRamLoad: LoadEstimator = null) =
    doWhileJava(
      toSerializablePredicate((in: JavaCollection[ConvOut]) => udf(JavaConversions.collectionAsScalaIterable(in))),
      new JavaFunction[DataQuanta[Out], RheemTuple[DataQuanta[Out], DataQuanta[ConvOut]]] {
        override def apply(t: DataQuanta[Out]) = {
          val result = bodyBuilder(t)
          new RheemTuple(result._1, result._2)
        }
      },
      numExpectedIterations, udfCpuLoad, udfRamLoad
    )

  /**
    * Feeds this instance into a do-while loop (guarded by a [[DoWhileOperator]].
    *
    * @param udf         condition to be evaluated after each iteration
    * @param bodyBuilder creates the loop body
    * @param udfCpuLoad  optional [[LoadEstimator]] for the CPU consumption of the `udf`
    * @param udfRamLoad  optional [[LoadEstimator]] for the RAM consumption of the `udf`
    * @return a new instance representing the final output of the [[DoWhileOperator]]
    */
  def doWhileJava[ConvOut: ClassTag](
                                      udf: SerializablePredicate[JavaCollection[ConvOut]],
                                      bodyBuilder: JavaFunction[DataQuanta[Out], RheemTuple[DataQuanta[Out], DataQuanta[ConvOut]]],
                                      numExpectedIterations: Int = 20,
                                      udfCpuLoad: LoadEstimator = null,
                                      udfRamLoad: LoadEstimator = null) = {
    // Create the DoWhileOperator.
    val doWhileOperator = new DoWhileOperator(
      dataSetType[Out],
      dataSetType[ConvOut],
      new PredicateDescriptor(udf, basicDataUnitType[JavaCollection[ConvOut]], null, udfCpuLoad, udfRamLoad),
      numExpectedIterations
    )
    this.connectTo(doWhileOperator, DoWhileOperator.INITIAL_INPUT_INDEX)

    // Create and wire the loop body.
    val loopDataQuanta = new DataQuanta[Out](doWhileOperator, DoWhileOperator.ITERATION_OUTPUT_INDEX)
    val iterationResults = bodyBuilder.apply(loopDataQuanta)
    iterationResults.getField0.connectTo(doWhileOperator, DoWhileOperator.ITERATION_INPUT_INDEX)
    iterationResults.getField1.connectTo(doWhileOperator, DoWhileOperator.CONVERGENCE_INPUT_INDEX)

    // Return the iteration result.
    new DataQuanta[Out](doWhileOperator, DoWhileOperator.FINAL_OUTPUT_INDEX)
  }

  /**
    * Feeds this instance into a for-loop (guarded by a [[LoopOperator]].
    *
    * @param n           number of iterations
    * @param bodyBuilder creates the loop body
    * @param udfCpuLoad  optional [[LoadEstimator]] for the CPU consumption of the `udf`
    * @param udfRamLoad  optional [[LoadEstimator]] for the RAM consumption of the `udf`
    * @return a new instance representing the final output of the [[LoopOperator]]
    */
  def repeat(n: Int,
             bodyBuilder: DataQuanta[Out] => DataQuanta[Out],
             udfCpuLoad: LoadEstimator = null,
             udfRamLoad: LoadEstimator = null) =
    repeatJava(n,
      new JavaFunction[DataQuanta[Out], DataQuanta[Out]] {
        override def apply(t: DataQuanta[Out]) = bodyBuilder(t)
      },
      udfCpuLoad,
      udfRamLoad
    )

  /**
    * Feeds this instance into a for-loop (guarded by a [[LoopOperator]].
    *
    * @param n           number of iterations
    * @param bodyBuilder creates the loop body
    * @param udfCpuLoad  optional [[LoadEstimator]] for the CPU consumption of the `udf`
    * @param udfRamLoad  optional [[LoadEstimator]] for the RAM consumption of the `udf`
    * @return a new instance representing the final output of the [[LoopOperator]]
    */
  def repeatJava(n: Int,
                 bodyBuilder: JavaFunction[DataQuanta[Out], DataQuanta[Out]],
                 udfCpuLoad: LoadEstimator = null,
                 udfRamLoad: LoadEstimator = null) = {
    // Create the DoWhileOperator.
    val loopOperator = new LoopOperator(
      dataSetType[Out],
      dataSetType[Int],
      new PredicateDescriptor(
        toSerializablePredicate[JavaCollection[Int]](i => RheemCollections.getSingle(i) >= n),
        basicDataUnitType[JavaCollection[Int]],
        null,
        udfCpuLoad,
        udfRamLoad
      ),
      n
    )
    this.connectTo(loopOperator, LoopOperator.INITIAL_INPUT_INDEX)
    new DataQuanta(CollectionSource.singleton[Int](0, classOf[Int])).connectTo(loopOperator, LoopOperator.INITIAL_CONVERGENCE_INPUT_INDEX)

    // Create and wire the main loop body.
    val loopDataQuanta = new DataQuanta[Out](loopOperator, LoopOperator.ITERATION_OUTPUT_INDEX)
    val iterationResult = bodyBuilder.apply(loopDataQuanta)
    iterationResult.connectTo(loopOperator, LoopOperator.ITERATION_INPUT_INDEX)

    // Create and wire the convergence loop body.
    val convergenceResult = new DataQuanta[Int](loopOperator, LoopOperator.ITERATION_CONVERGENCE_OUTPUT_INDEX).map(_ + 1)
    convergenceResult.connectTo(loopOperator, LoopOperator.ITERATION_CONVERGENCE_INPUT_INDEX)

    // Return the iteration result.
    new DataQuanta[Out](loopOperator, LoopOperator.FINAL_OUTPUT_INDEX)
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
  def foreach(f: Out => _, jobName: String = null): Unit = foreachJava(toConsumer(f), jobName)

  /**
    * Perform a local action on each data quantum in this instance. Triggers execution.
    *
    * @param f the action to perform as Java 8 lambda expression
    */
  def foreachJava(f: Consumer[Out], jobName: String = null): Unit = {
    val sink = new LocalCallbackSink(f, dataSetType[Out])
    sink.setName("foreach()")
    this.connectTo(sink, 0)
    this.planBuilder.sinks += sink
    this.planBuilder.buildAndExecute(jobName)
    this.planBuilder.sinks.clear()
  }

  /**
    * Collect the data quanta in this instance. Triggers execution.
    *
    * @return the data quanta
    */
  def collect(jobName: String = null): Iterable[Out] = {
    // Set up the sink.
    val collector = new java.util.LinkedList[Out]()
    val sink = LocalCallbackSink.createCollectingSink(collector, dataSetType[Out])
    sink.setName("collect()")
    this.connectTo(sink, 0)

    // Do the execution.
    this.planBuilder.sinks += sink
    this.planBuilder.buildAndExecute(jobName)
    this.planBuilder.sinks.clear()

    // Return the collected values.
    collector
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
    this.planBuilder.udfJars ++= paths
    this
  }

  /**
    * Defines user-code JAR files that might be needed to transfer to execution platforms.
    *
    * @param classes whose JAR files should be transferred
    * @return this instance
    */
  def withUdfJarsOf(classes: Class[_]*) =
    withUdfJars(classes.map(ReflectionUtils.getDeclaringJar).filterNot(_ == null): _*)


}

object DataQuanta {

  def create[T](output: OutputSlot[T])(implicit planBuilder: PlanBuilder): DataQuanta[_] =
    new DataQuanta(output.getOwner.asInstanceOf[ElementaryOperator], output.getIndex)(ClassTag(output.getType.getDataUnitType.getTypeClass), planBuilder)

}
