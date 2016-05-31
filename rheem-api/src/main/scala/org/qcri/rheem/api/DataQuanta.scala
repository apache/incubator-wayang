package org.qcri.rheem.api

import _root_.java.util.function.Consumer

import org.apache.commons.lang3.Validate
import org.qcri.rheem.basic.operators._
import org.qcri.rheem.core.function.FunctionDescriptor.{SerializableBinaryOperator, SerializableFunction}
import org.qcri.rheem.core.function.PredicateDescriptor.SerializablePredicate
import org.qcri.rheem.core.function.{FlatMapDescriptor, PredicateDescriptor, ReduceDescriptor, TransformationDescriptor}
import org.qcri.rheem.core.plan.rheemplan.{Operator, RheemPlan}

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
class DataQuanta[Out: ClassTag](operator: Operator)(implicit planBuilder: PlanBuilder) {

  Validate.isTrue(operator.getNumOutputs == 1)

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
    this.operator.connectTo(0, mapOperator, 0)
    mapOperator
  }

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
    this.operator.connectTo(0, filterOperator, 0)
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
    this.operator.connectTo(0, flatMapOperator, 0)
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
    this.operator.connectTo(0, reduceByOperator, 0)
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
    this.operator.connectTo(0, globalReduceOperator, 0)
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
    this.operator.connectTo(0, unionAllOperator, 0)
    that._operator.connectTo(0, unionAllOperator, 1)
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
    this.operator.connectTo(0, joinOperator, 0)
    that._operator.connectTo(0, joinOperator, 1)
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
    this.operator.connectTo(0, cartesianOperator, 0)
    that._operator.connectTo(0, cartesianOperator, 1)
    cartesianOperator
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
    this.operator.connectTo(0, sink, 0)
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
    this.operator.connectTo(0, sink, 0)

    // Do the execution.
    this.planBuilder.sinks += sink
    this.planBuilder.buildAndExecute()
    this.planBuilder.sinks.clear()

    // Return the collected values.
    collector
  }

  private def _operator = operator

}
