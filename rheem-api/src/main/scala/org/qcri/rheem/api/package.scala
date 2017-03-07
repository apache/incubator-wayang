package org.qcri.rheem

import _root_.java.lang.{Class => JavaClass, Iterable => JavaIterable}
import _root_.java.util.function.{Consumer, ToLongBiFunction, ToLongFunction}

import org.qcri.rheem.basic.data.{Record, Tuple2 => RheemTuple2}
import org.qcri.rheem.core.api.RheemContext
import org.qcri.rheem.core.function.FunctionDescriptor.{SerializableBinaryOperator, SerializableFunction, SerializablePredicate}
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval
import org.qcri.rheem.core.optimizer.cardinality.{CardinalityEstimate, CardinalityEstimator, DefaultCardinalityEstimator, FixedSizeCardinalityEstimator}
import org.qcri.rheem.core.optimizer.costs.{DefaultLoadEstimator, LoadEstimator, LoadProfileEstimator, NestableLoadProfileEstimator}
import org.qcri.rheem.core.plan.rheemplan.ElementaryOperator
import org.qcri.rheem.core.types.{BasicDataUnitType, DataSetType, DataUnitGroupType, DataUnitType}

import scala.collection.JavaConversions
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * Provides implicits for the basic Rheem API.
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

  implicit def dataSetType[T](implicit classTag: ClassTag[T]): DataSetType[T] =
    DataSetType.createDefault(basicDataUnitType[T])

  implicit def groupedDataSetType[T](implicit classTag: ClassTag[T]): DataSetType[JavaIterable[T]] =
    DataSetType.createGrouped(basicDataUnitType[T])


  implicit def toSerializableFunction[In, Out](scalaFunc: In => Out): SerializableFunction[In, Out] =
    new SerializableFunction[In, Out] {
      override def apply(t: In) = scalaFunc(t)
    }

  implicit def toJoinedDataQuanta[Out0: ClassTag, Out1: ClassTag](dataQuanta: DataQuanta[RheemTuple2[Out0, Out1]]):
  JoinedDataQuanta[Out0, Out1] =
    new JoinedDataQuanta(dataQuanta)

  implicit def toSerializablePartitionFunction[In, Out](scalaFunc: Iterable[In] => Iterable[Out]):
  SerializableFunction[JavaIterable[In], JavaIterable[Out]] =
    new SerializableFunction[JavaIterable[In], JavaIterable[Out]] {
      override def apply(t: JavaIterable[In]) = JavaConversions.asJavaIterable(scalaFunc(JavaConversions.iterableAsScalaIterable(t)))
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

  implicit def toCardinalityEstimator(fixCardinality: Long): CardinalityEstimator =
    new FixedSizeCardinalityEstimator(fixCardinality, true)

  implicit def toCardinalityEstimator(fixCardinality: Int): CardinalityEstimator =
    new FixedSizeCardinalityEstimator(fixCardinality, true)

  implicit def toCardinalityEstimator(f: Long => Long): CardinalityEstimator =
    new DefaultCardinalityEstimator(.99d, 1, true, new ToLongFunction[Array[Long]] {
      override def applyAsLong(inCards: Array[Long]): Long = f.apply(inCards(0))
    })

  implicit def toCardinalityEstimator(f: (Long, Long) => Long): CardinalityEstimator =
    new DefaultCardinalityEstimator(.99d, 1, true, new ToLongFunction[Array[Long]] {
      override def applyAsLong(inCards: Array[Long]): Long = f.apply(inCards(0), inCards(1))
    })

  implicit def toLoadEstimator(f: (Long, Long) => Long): LoadEstimator =
    new DefaultLoadEstimator(
      1,
      1,
      .99d,
      CardinalityEstimate.EMPTY_ESTIMATE,
      new ToLongBiFunction[Array[Long], Array[Long]] {
        override def applyAsLong(t: Array[Long], u: Array[Long]): Long = f.apply(t(0), u(0))
      }
    )

  implicit def toLoadEstimator(f: (Long, Long, Long) => Long): LoadEstimator =
    new DefaultLoadEstimator(
      2,
      1,
      .99d,
      CardinalityEstimate.EMPTY_ESTIMATE,
      new ToLongBiFunction[Array[Long], Array[Long]] {
        override def applyAsLong(t: Array[Long], u: Array[Long]): Long = f.apply(t(0), t(1), u(0))
      }
    )

  implicit def toLoadProfileEstimator(f: (Long, Long) => Long): LoadProfileEstimator =
    new NestableLoadProfileEstimator(f, (in: Long, out: Long) => 0L)

  implicit def toLoadProfileEstimator(f: (Long, Long, Long) => Long): LoadProfileEstimator =
    new NestableLoadProfileEstimator(f, (in0: Long, in1: Long, out: Long) => 0L)


  implicit def toInterval(double: Double): ProbabilisticDoubleInterval = new ProbabilisticDoubleInterval(double, double, .99)

  implicit def createPlanBuilder(rheemContext: RheemContext): PlanBuilder = new PlanBuilder(rheemContext)

  implicit private[api] def wrap[Out: ClassTag](op: ElementaryOperator)(implicit planBuilder: PlanBuilder): DataQuanta[Out] =
    new DataQuanta[Out](op)

  implicit def elevateRecordDataQuanta(dataQuanta: DataQuanta[Record]): RecordDataQuanta =
    new RecordDataQuanta(dataQuanta)

}
