package org.qcri.rheem.api

import org.apache.commons.lang3.Validate
import org.qcri.rheem.api
import org.qcri.rheem.basic.operators.{CollectionSource, TextFileSource}
import org.qcri.rheem.core.api.RheemContext
import org.qcri.rheem.core.plan.rheemplan.{Operator, RheemPlan}

import scala.collection.JavaConversions
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions
import scala.reflect._

/**
  * Utility to build [[RheemPlan]]s.
  */
class PlanBuilder(rheemContext: RheemContext) {

  private[api] val sinks = ListBuffer[Operator]()

  private[api] val udfJars = scala.collection.mutable.Set[String]()

  def buildAndExecute(): Unit = {
    val plan: RheemPlan = new RheemPlan(this.sinks.toArray: _*)
    this.rheemContext.execute(plan, this.udfJars.toArray:_*)
  }

  def readTextFile(url: String): DataQuanta[String] =
    new TextFileSource(url)

  def readCollection[T: ClassTag](collection: java.util.Collection[T]): DataQuanta[T] =
    new CollectionSource[T](collection, dataSetType[T])

  def readCollection[T: ClassTag](iterable: Iterable[T]): DataQuanta[T] =
    readCollection(JavaConversions.asJavaCollection(iterable))

  def customOperator(operator: Operator, inputs: DataQuanta[_]*): IndexedSeq[DataQuanta[_]] = {
    Validate.isTrue(operator.getNumRegularInputs == inputs.size)

    // Set up inputs.
    inputs.zipWithIndex.foreach(zipped => zipped._1.connectTo(operator, zipped._2))

    // Set up outputs.
    for (outputIndex <- 0 until operator.getNumOutputs) yield DataQuanta.create(operator.getOutput(outputIndex))(this)
  }

  implicit private[api] def wrap[T : ClassTag](operator: Operator): DataQuanta[T] =
    PlanBuilder.wrap[T](operator)(classTag[T], this)

}

object PlanBuilder {

  implicit private[api] def wrap[T : ClassTag](operator: Operator)(implicit planBuilder: PlanBuilder): DataQuanta[T] =
    api.wrap[T](operator)

}
