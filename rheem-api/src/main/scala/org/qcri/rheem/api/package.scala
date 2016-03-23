package org.qcri.rheem

import _root_.java.lang.{Class => JavaClass}
import _root_.java.util.function.Consumer

import org.qcri.rheem.core.function.FunctionDescriptor.SerializableFunction
import org.qcri.rheem.core.plan.rheemplan.Operator
import org.qcri.rheem.core.types.{BasicDataUnitType, DataSetType, DataUnitType}

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * Created by basti on 03/22/16.
  */
package object api {

  implicit def basicDataUnitType[T](implicit classTag: ClassTag[T]): BasicDataUnitType[T] = {
    val cls = classTag.runtimeClass.asInstanceOf[JavaClass[T]]
    DataUnitType.createBasic(cls)
  }

  implicit def dataSetType[T](implicit classTag: ClassTag[T]): DataSetType[T] = {
    DataSetType.createDefault(basicDataUnitType[T])
  }

  implicit def toSerializableFunction[In, Out](scalaFunc: In => Out): SerializableFunction[In, Out] =
    new SerializableFunction[In, Out] {
      override def apply(t: In): Out = scalaFunc(t)
    }

  implicit def toConsumer[T](scalaFunc: T => _): Consumer[T] = {
    new Consumer[T] {
      def accept(t: T) = scalaFunc.apply(t)
    }
  }

  implicit private[api] def wrap[Out: ClassTag](op: Operator)(implicit planBuilder: PlanBuilder): DataQuanta[Out] =
    new DataQuanta[Out](op)

}
