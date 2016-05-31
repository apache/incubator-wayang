package org.qcri.rheem.api

import _root_.java.util.function.{Consumer, Function => JavaFunction}
import _root_.java.util.{Collection => JavaCollection}

import org.apache.commons.lang3.Validate
import org.qcri.rheem.basic.operators._
import org.qcri.rheem.core.function.FunctionDescriptor.{SerializableBinaryOperator, SerializableFunction}
import org.qcri.rheem.core.function.PredicateDescriptor.SerializablePredicate
import org.qcri.rheem.core.function.{FlatMapDescriptor, PredicateDescriptor, ReduceDescriptor, TransformationDescriptor}
import org.qcri.rheem.core.plan.rheemplan.{InputSlot, Operator, RheemPlan}
import org.qcri.rheem.core.util.{RheemCollections, Tuple => RheemTuple}

import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import scala.reflect._

/**
  * Represents an intermediate result/data flow edge in a [[RheemPlan]].
  *
  * @param operator    a unary [[Operator]] that produces this instance
  * @param ev$1        the data type of the elements in this instance
  * @param planBuilder keeps track of the [[RheemPlan]] being build
  * @tparam Out
  */
class DataQuanta[Out: ClassTag](operator: Operator, outputIndex: Int = 0)(implicit planBuilder: PlanBuilder) {

  Validate.isTrue(operator.getNumOutputs > outputIndex)

  /**
    * Feed this instance into a [[MapOperator]].
    *
    * @param udf UDF for the [[MapOperator]]
    * @return a new instance representing the [[MapOperator]]'s output
    */
  def map[NewOut: ClassTag](udf: Out => NewOut): DataQuanta[NewOut] = mapJava(toSerializableFunction(udf))

  /**
    * Feed this instance into a [[MapOperator]].
    *
    * @param udf a Java 8 lambda expression as UDF for the [[MapOperator]]
    * @return a new instance representing the [[MapOperator]]'s output
    */
  def mapJava[NewOut: ClassTag](udf: SerializableFunction[Out, NewOut]): DataQuanta[NewOut] = {
    val mapOperator = new MapOperator(new TransformationDescriptor(
      udf, basicDataUnitType[Out], basicDataUnitType[NewOut]
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
  protected def connectTo(operator: Operator, inputIndex: Int): Unit =
    this.operator.connectTo(outputIndex, operator, inputIndex)


  /**
    * Feed this instance into a [[FilterOperator]].
    *
    * @param udf UDF for the [[FilterOperator]]
    * @return a new instance representing the [[FilterOperator]]'s output
    */
  def filter(udf: Out => Boolean) = filterJava(toSerializablePredicate(udf))

  /**
    * Feed this instance into a [[FilterOperator]].
    *
    * @param udf UDF for the [[FilterOperator]]
    * @return a new instance representing the [[FilterOperator]]'s output
    */
  def filterJava(udf: SerializablePredicate[Out]): DataQuanta[Out] = {
    val filterOperator = new FilterOperator(new PredicateDescriptor(udf, basicDataUnitType[Out]))
    this.connectTo(filterOperator, 0)
    filterOperator
  }

  /**
    * Feed this instance into a [[FlatMapOperator]].
    *
    * @param udf UDF for the [[FlatMapOperator]]
    * @return a new instance representing the [[FlatMapOperator]]'s output
    */
  def flatMap[NewOut: ClassTag](udf: Out => Iterable[NewOut]): DataQuanta[NewOut] =
    flatMapJava(toSerializableFlatteningFunction(udf))

  /**
    * Feed this instance into a [[FlatMapOperator]].
    *
    * @param udf a Java 8 lambda expression as UDF for the [[FlatMapOperator]]
    * @return a new instance representing the [[FlatMapOperator]]'s output
    */
  def flatMapJava[NewOut: ClassTag](udf: SerializableFunction[Out, java.lang.Iterable[NewOut]]): DataQuanta[NewOut] = {
    val flatMapOperator = new FlatMapOperator(new FlatMapDescriptor(
      udf, basicDataUnitType[Out], basicDataUnitType[NewOut]
    ))
    this.connectTo(flatMapOperator, 0)
    flatMapOperator
  }

  /**
    * Feed this instance into a [[ReduceByOperator]].
    *
    * @param keyUdf UDF to extract the grouping key from the data quanta
    * @param udf    aggregation UDF for the [[ReduceByOperator]]
    * @return a new instance representing the [[ReduceByOperator]]'s output
    */
  def reduceByKey[Key: ClassTag](keyUdf: Out => Iterable[Key], udf: (Out, Out) => Out): DataQuanta[Out] =
    reduceByKeyJava(toSerializableFunction(keyUdf), toSerializableBinaryOperator(udf))

  /**
    * Feed this instance into a [[ReduceByOperator]].
    *
    * @param keyUdf UDF to extract the grouping key from the data quanta
    * @param udf    aggregation UDF for the [[ReduceByOperator]]
    * @return a new instance representing the [[ReduceByOperator]]'s output
    */
  def reduceByKeyJava[Key: ClassTag](keyUdf: SerializableFunction[Out, Key], udf: SerializableBinaryOperator[Out])
  : DataQuanta[Out] = {
    val reduceByOperator = new ReduceByOperator(
      new TransformationDescriptor(keyUdf, basicDataUnitType[Out], basicDataUnitType[Key]),
      new ReduceDescriptor(udf, groupedDataUnitType[Out], basicDataUnitType[Out])
    )
    this.connectTo(reduceByOperator, 0)
    reduceByOperator
  }

  /**
    * Feed this instance into a [[GlobalReduceOperator]].
    *
    * @param udf aggregation UDF for the [[GlobalReduceOperator]]
    * @return a new instance representing the [[GlobalReduceOperator]]'s output
    */
  def reduce(udf: (Out, Out) => Out): DataQuanta[Out] =
    reduceJava(toSerializableBinaryOperator(udf))

  /**
    * Feed this instance into a [[GlobalReduceOperator]].
    *
    * @param udf aggregation UDF for the [[GlobalReduceOperator]]
    * @return a new instance representing the [[GlobalReduceOperator]]'s output
    */
  def reduceJava(udf: SerializableBinaryOperator[Out]): DataQuanta[Out] = {
    val globalReduceOperator = new GlobalReduceOperator(
      new ReduceDescriptor(udf, groupedDataUnitType[Out], basicDataUnitType[Out])
    )
    this.connectTo(globalReduceOperator, 0)
    globalReduceOperator
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
    * Feeds this instance into a do-while loop (guarded by a [[DoWhileOperator]].
    *
    * @param udf         condition to be evaluated after each iteration
    * @param bodyBuilder creates the loop body
    * @return a new instance representing the final output of the [[DoWhileOperator]]
    */
  def doWhile[ConvOut: ClassTag](udf: Iterable[ConvOut] => Boolean,
                                 bodyBuilder: DataQuanta[Out] => (DataQuanta[Out], DataQuanta[ConvOut])) =
    doWhileJava(
      toSerializablePredicate((in: JavaCollection[ConvOut]) => udf(JavaConversions.collectionAsScalaIterable(in))),
      new JavaFunction[DataQuanta[Out], RheemTuple[DataQuanta[Out], DataQuanta[ConvOut]]] {
        override def apply(t: DataQuanta[Out]) = {
          val result = bodyBuilder(t)
          new RheemTuple(result._1, result._2)
        }
      }
    )

  /**
    * Feeds this instance into a do-while loop (guarded by a [[DoWhileOperator]].
    *
    * @param udf         condition to be evaluated after each iteration
    * @param bodyBuilder creates the loop body
    * @return a new instance representing the final output of the [[DoWhileOperator]]
    */
  def doWhileJava[ConvOut: ClassTag](
                                      udf: SerializablePredicate[JavaCollection[ConvOut]],
                                      bodyBuilder: JavaFunction[DataQuanta[Out], RheemTuple[DataQuanta[Out], DataQuanta[ConvOut]]]
                                    ) = {
    // Create the DoWhileOperator.
    val doWhileOperator = new DoWhileOperator(
      dataSetType[Out],
      dataSetType[ConvOut],
      new PredicateDescriptor(udf, basicDataUnitType[java.util.Collection[ConvOut]])
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
    // Create the DoWhileOperator.
    val loopOperator = new LoopOperator(
      dataSetType[Out], dataSetType[Int], new PredicateDescriptor(
        toSerializablePredicate[JavaCollection[Int]](i => RheemCollections.getSingle(i) >= n), basicDataUnitType[JavaCollection[Int]])
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
  def foreachJava(f: Consumer[Out]): Unit = {
    val sink = new LocalCallbackSink(f, dataSetType[Out])
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
  def collect(): Iterable[Out] = {
    // Set up the sink.
    val collector = new java.util.LinkedList[Out]()
    val sink = LocalCallbackSink.createCollectingSink(collector, dataSetType[Out])
    this.connectTo(sink, 0)

    // Do the execution.
    this.planBuilder.sinks += sink
    this.planBuilder.buildAndExecute()
    this.planBuilder.sinks.clear()

    // Return the collected values.
    collector
  }

}
