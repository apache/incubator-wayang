package org.qcri.rheem.api

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

  def buildAndExecute(): Unit = {
    this.rheemContext.execute(new RheemPlan(this.sinks.toArray: _*))
  }

  def readTextFile(url: String): DataQuanta[String] =
    new TextFileSource(url)

  def readCollection[T: ClassTag](collection: java.util.Collection[T]): DataQuanta[T] =
    new CollectionSource[T](collection, dataSetType[T])

  def readCollection[T: ClassTag](iterable: Iterable[T]): DataQuanta[T] =
    readCollection(JavaConversions.asJavaCollection(iterable))

  implicit private[api] def wrap[T : ClassTag](operator: Operator): DataQuanta[T] =
    PlanBuilder.wrap[T](operator)(classTag[T], this)

}

object PlanBuilder {

  implicit private[api] def wrap[T : ClassTag](operator: Operator)(implicit planBuilder: PlanBuilder): DataQuanta[T] =
    api.wrap[T](operator)

}
