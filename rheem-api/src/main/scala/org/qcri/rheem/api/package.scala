package org.qcri.rheem

import _root_.java.lang.{Class => JavaClass, Iterable => JavaIterable}
import _root_.java.util.function.Consumer

import org.qcri.rheem.core.api.RheemContext
import org.qcri.rheem.core.function.FunctionDescriptor.{SerializableBinaryOperator, SerializableFunction}
import org.qcri.rheem.core.function.PredicateDescriptor.SerializablePredicate
import org.qcri.rheem.core.plan.rheemplan.Operator
import org.qcri.rheem.core.types.{BasicDataUnitType, DataSetType, DataUnitGroupType, DataUnitType}

import scala.collection.JavaConversions
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

  implicit def groupedDataUnitType[T](implicit classTag: ClassTag[T]): DataUnitGroupType[T] = {
    val cls = classTag.runtimeClass.asInstanceOf[JavaClass[T]]
    DataUnitType.createGrouped(cls)
  }

  implicit def dataSetType[T](implicit classTag: ClassTag[T]): DataSetType[T] = {
    DataSetType.createDefault(basicDataUnitType[T])
  }

  implicit def toSerializableFunction[In, Out](scalaFunc: In => Out): SerializableFunction[In, Out] =
    new SerializableFunction[In, Out] {
      override def apply(t: In) = scalaFunc(t)
    }

  implicit def toSerializablePredicate[T](scalaFunc: T => Boolean): SerializablePredicate[T] =
    new SerializablePredicate[T] {
      override def test(t: T) = scalaFunc(t)
    }

  implicit def toSerializableFlatteningFunction[In, Out](scalaFunc: In => Iterable[Out]): SerializableFunction[In, JavaIterable[Out]] =
    new SerializableFunction[In, JavaIterable[Out]] {
      override def apply(t: In) = JavaConversions.asJavaIterable(scalaFunc(t))
    }

  implicit def toSerializableBinaryOperator[T](scalaFunc: (T, T) => T): SerializableBinaryOperator[T] =
    new SerializableBinaryOperator[T] {
      override def apply(t1: T, t2: T) = scalaFunc(t1, t2)
    }

  implicit def toConsumer[T](scalaFunc: T => _): Consumer[T] = {
    new Consumer[T] {
      def accept(t: T) = scalaFunc.apply(t)
    }
  }

  implicit def createPlanBuilder(rheemContext: RheemContext): PlanBuilder = new PlanBuilder(rheemContext)

  implicit private[api] def wrap[Out: ClassTag](op: Operator)(implicit planBuilder: PlanBuilder): DataQuanta[Out] =
    new DataQuanta[Out](op)

}
