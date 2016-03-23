package org.qcri.rheem.api

import _root_.java.util.function.Consumer

import org.apache.commons.lang3.Validate
import org.qcri.rheem.basic.operators.{LocalCallbackSink, MapOperator}
import org.qcri.rheem.core.function.FunctionDescriptor.SerializableFunction
import org.qcri.rheem.core.function.TransformationDescriptor
import org.qcri.rheem.core.plan.rheemplan.Operator

import scala.collection.JavaConversions._
import scala.reflect._

class DataQuanta[Out: ClassTag](operator: Operator)(implicit planBuilder: PlanBuilder) {

  Validate.isTrue(operator.getNumOutputs == 1)

  def map[NewOut: ClassTag](mapFunction: Out => NewOut): DataQuanta[NewOut] = mapJava(toSerializableFunction(mapFunction))

  def mapJava[NewOut: ClassTag](mapFunction: SerializableFunction[Out, NewOut]): DataQuanta[NewOut] = {
    val mapOperator = new MapOperator(new TransformationDescriptor(
      mapFunction, basicDataUnitType[Out], basicDataUnitType[NewOut]
    ))
    this.operator.connectTo(0, mapOperator, 0)
    mapOperator
  }

  def foreach(f: Out => _): Unit = foreachJava(toConsumer(f))

  def foreachJava(f: Consumer[Out]): Unit = {
    val sink = new LocalCallbackSink(f, dataSetType[Out])
    this.operator.connectTo(0, sink, 0)
    this.planBuilder.sinks += sink
    this.planBuilder.buildAndExecute()
    this.planBuilder.sinks.clear()
  }

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

}
