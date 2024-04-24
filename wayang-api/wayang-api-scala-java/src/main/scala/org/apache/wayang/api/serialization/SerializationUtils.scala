/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.api.serialization

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation._
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.wayang.api.serialization.customserializers._
import org.apache.wayang.api.serialization.mixins.ConfigurationAndContextMixIns._
import org.apache.wayang.api.serialization.mixins.DataTypeMixIns._
import org.apache.wayang.api.serialization.mixins.DescriptorMixIns._
import org.apache.wayang.api.serialization.mixins.EstimatorMixIns._
import org.apache.wayang.api.serialization.mixins.IgnoreLoggerMixIn
import org.apache.wayang.api.serialization.mixins.OperatorMixIns._
import org.apache.wayang.api.serialization.mixins.ProviderMixIns._
import org.apache.wayang.api.serialization.mixins.SlotMixIns._
import org.apache.wayang.api.{MultiContext, MultiContextPlanBuilder}
import org.apache.wayang.basic.function.ProjectionDescriptor
import org.apache.wayang.basic.operators._
import org.apache.wayang.basic.types.RecordType
import org.apache.wayang.core.api.configuration._
import org.apache.wayang.core.api.{Configuration, Job, WayangContext}
import org.apache.wayang.core.function.FunctionDescriptor._
import org.apache.wayang.core.function._
import org.apache.wayang.core.mapping.{OperatorPattern, PlanTransformation}
import org.apache.wayang.core.optimizer.cardinality.{CardinalityEstimate, CardinalityEstimator, CardinalityEstimatorManager, CardinalityPusher, DefaultCardinalityEstimator}
import org.apache.wayang.core.optimizer.channels.ChannelConversionGraph
import org.apache.wayang.core.optimizer.costs._
import org.apache.wayang.core.optimizer.enumeration._
import org.apache.wayang.core.optimizer.{OptimizationContext, OptimizationUtils, ProbabilisticDoubleInterval, SanityChecker}
import org.apache.wayang.core.plan.executionplan.{Channel, ExecutionPlan}
import org.apache.wayang.core.plan.wayangplan._
import org.apache.wayang.core.plan.wayangplan.traversal.AbstractTopologicalTraversal
import org.apache.wayang.core.platform._
import org.apache.wayang.core.profiling.{CardinalityRepository, ExecutionLog}
import org.apache.wayang.core.types.{BasicDataUnitType, DataSetType, DataUnitGroupType, DataUnitType}
import org.apache.wayang.core.util.fs.{FileSystems, HadoopFileSystem, LocalFileSystem}
import org.apache.wayang.core.util.{AbstractReferenceCountable, ReflectionUtils}

import scala.reflect.ClassTag

object SerializationUtils {

  val mapper: ObjectMapper = {
    val mapper = new ObjectMapper()
      .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
      .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .enable(SerializationFeature.INDENT_OUTPUT)
      .registerModule(DefaultScalaModule)

      // Custom serializers
      .registerModule(new SimpleModule().addSerializer(classOf[MultiContext], new MultiContextSerializer()))
      .registerModule(new SimpleModule().addDeserializer(classOf[MultiContext], new MultiContextDeserializer()))
      .registerModule(new SimpleModule().addSerializer(classOf[Platform], new PlatformSerializer()))
      .registerModule(new SimpleModule().addDeserializer(classOf[Platform], new PlatformDeserializer()))
      .registerModule(new SimpleModule().addDeserializer(classOf[Operator], new OperatorDeserializer()))

      // Custom serializers for UDFs
      .registerModule(new SimpleModule().addSerializer(classOf[SerializablePredicate[_]], new GenericSerializableSerializer[SerializablePredicate[_]]()))
      .registerModule(new SimpleModule().addDeserializer(classOf[SerializablePredicate[_]], new GenericSerializableDeserializer[SerializablePredicate[_]]()))
      .registerModule(new SimpleModule().addSerializer(classOf[SerializableFunction[_, _]], new GenericSerializableSerializer[FunctionDescriptor.SerializableFunction[_, _]]()))
      .registerModule(new SimpleModule().addDeserializer(classOf[SerializableFunction[_, _]], new GenericSerializableDeserializer[FunctionDescriptor.SerializableFunction[_, _]]()))
      .registerModule(new SimpleModule().addSerializer(classOf[SerializableBinaryOperator[_]], new GenericSerializableSerializer[SerializableBinaryOperator[_]]()))
      .registerModule(new SimpleModule().addDeserializer(classOf[SerializableBinaryOperator[_]], new GenericSerializableDeserializer[SerializableBinaryOperator[_]]()))
      .registerModule(new SimpleModule().addSerializer(classOf[SerializableConsumer[_]], new GenericSerializableSerializer[SerializableConsumer[_]]()))
      .registerModule(new SimpleModule().addDeserializer(classOf[SerializableConsumer[_]], new GenericSerializableDeserializer[SerializableConsumer[_]]()))
      .registerModule(new SimpleModule().addSerializer(classOf[SerializableIntUnaryOperator], new GenericSerializableSerializer[SerializableIntUnaryOperator]()))
      .registerModule(new SimpleModule().addDeserializer(classOf[SerializableIntUnaryOperator], new GenericSerializableDeserializer[SerializableIntUnaryOperator]()))
      .registerModule(new SimpleModule().addSerializer(classOf[SerializableLongUnaryOperator], new GenericSerializableSerializer[SerializableLongUnaryOperator]()))
      .registerModule(new SimpleModule().addDeserializer(classOf[SerializableLongUnaryOperator], new GenericSerializableDeserializer[SerializableLongUnaryOperator]()))
      .registerModule(new SimpleModule().addSerializer(classOf[LoadEstimator.SinglePointEstimationFunction], new GenericSerializableSerializer[LoadEstimator.SinglePointEstimationFunction]()))
      .registerModule(new SimpleModule().addDeserializer(classOf[LoadEstimator.SinglePointEstimationFunction], new GenericSerializableDeserializer[LoadEstimator.SinglePointEstimationFunction]()))
      .registerModule(new SimpleModule().addSerializer(classOf[SerializableToLongBiFunction[_, _]], new GenericSerializableSerializer[SerializableToLongBiFunction[_, _]]()))
      .registerModule(new SimpleModule().addDeserializer(classOf[SerializableToLongBiFunction[_, _]], new GenericSerializableDeserializer[SerializableToLongBiFunction[_, _]]()))
      .registerModule(new SimpleModule().addSerializer(classOf[SerializableToDoubleBiFunction[_, _]], new GenericSerializableSerializer[SerializableToDoubleBiFunction[_, _]]()))
      .registerModule(new SimpleModule().addDeserializer(classOf[SerializableToDoubleBiFunction[_, _]], new GenericSerializableDeserializer[SerializableToDoubleBiFunction[_, _]]()))

    // Register mix-ins
    mapper
      .addMixIn(classOf[MultiContextPlanBuilder], classOf[MultiContextPlanBuilderMixIn])
      .addMixIn(classOf[WayangContext], classOf[WayangContextMixIn])
      .addMixIn(classOf[Configuration], classOf[ConfigurationMixIn])
      .addMixIn(classOf[CardinalityRepository], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[KeyValueProvider[_, _]], classOf[KeyValueProviderMixIn])
      .addMixIn(classOf[ValueProvider[_]], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[CollectionProvider[_]], classOf[CollectionProviderMixIn])
      .addMixIn(classOf[ExplicitCollectionProvider[_]], classOf[ExplicitCollectionProviderMixIn])
      .addMixIn(classOf[FunctionalKeyValueProvider[_, _]], classOf[FunctionalKeyValueProviderMixIn[_, _]])
      .addMixIn(classOf[MapBasedKeyValueProvider[_, _]], classOf[MapBasedKeyValueProviderMixIn[_, _]])
      .addMixIn(classOf[ConstantValueProvider[_]], classOf[ConstantValueProviderMixIn])
      .addMixIn(classOf[PlanTransformation], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[OperatorPattern[_]], classOf[OperatorPatternMixin])
      .addMixIn(classOf[Slot[_]], classOf[SlotMixIn[_]])
      .addMixIn(classOf[OutputSlot[_]], classOf[OutputSlotMixIn[_]])
      .addMixIn(classOf[OperatorBase], classOf[OperatorBaseMixIn])
      .addMixIn(classOf[MultiContext.UnarySink], classOf[MultiContextUnarySinkMixIn])
      .addMixIn(classOf[ElementaryOperator], classOf[ElementaryOperatorMixIn])
      .addMixIn(classOf[ActualOperator], classOf[ActualOperatorMixIn])
      .addMixIn(classOf[Operator], classOf[OperatorMixIn])
      .addMixIn(classOf[PredicateDescriptor[_]], classOf[PredicateDescriptorMixIn[_]])
      .addMixIn(classOf[TransformationDescriptor[_, _]], classOf[TransformationDescriptorMixIn[_, _]])
      .addMixIn(classOf[ProjectionDescriptor[_, _]], classOf[ProjectionDescriptorMixIn[_, _]])
      .addMixIn(classOf[ReduceDescriptor[_]], classOf[ReduceDescriptorMixIn[_]])
      .addMixIn(classOf[FlatMapDescriptor[_, _]], classOf[FlatMapDescriptorMixIn[_, _]])
      .addMixIn(classOf[MapPartitionsDescriptor[_, _]], classOf[MapPartitionsDescriptorMixIn[_, _]])
      .addMixIn(classOf[BasicDataUnitType[_]], classOf[BasicDataUnitTypeMixIn[_]])
      .addMixIn(classOf[RecordType], classOf[RecordTypeMixIn])
      .addMixIn(classOf[DataUnitGroupType[_]], classOf[DataUnitGroupTypeMixIn[_]])
      .addMixIn(classOf[ProbabilisticDoubleInterval], classOf[ProbabilisticDoubleIntervalMixIn])
      .addMixIn(classOf[LoadProfileEstimator], classOf[LoadProfileEstimatorMixIn])
      .addMixIn(classOf[FunctionDescriptor], classOf[FunctionDescriptorMixIn])
      .addMixIn(classOf[NestableLoadProfileEstimator], classOf[NestableLoadProfileEstimatorMixIn])
      .addMixIn(classOf[LoadEstimator], classOf[LoadEstimatorMixIn])
      .addMixIn(classOf[DefaultLoadEstimator], classOf[DefaultLoadEstimatorMixIn])
      .addMixIn(classOf[CardinalityEstimate], classOf[CardinalityEstimateMixIn])
      .addMixIn(classOf[DataSetType[_]], classOf[DataSetTypeMixIn[_]])
      .addMixIn(classOf[DataUnitType[_]], classOf[DataUnitTypeMixIn])
      .addMixIn(classOf[TextFileSource], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[UnarySource[_]], classOf[UnarySourceMixIn[_]])
      .addMixIn(classOf[UnarySink[_]], classOf[UnarySinkMixIn[_]])
      .addMixIn(classOf[UnaryToUnaryOperator[_, _]], classOf[UnaryToUnaryOperatorMixIn[_, _]])
      .addMixIn(classOf[BinaryToUnaryOperator[_, _, _]], classOf[BinaryToUnaryOperatorMixIn[_, _, _]])
      .addMixIn(classOf[LoopHeadOperator], classOf[LoopHeadOperatorMixIn])
      .addMixIn(classOf[SampleOperator[_]], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[CardinalityEstimator], classOf[CardinalityEstimatorMixIn])
      .addMixIn(classOf[DefaultCardinalityEstimator], classOf[DefaultCardinalityEstimatorMixIn])
      .addMixIn(classOf[EstimatableCost], classOf[EstimatableCostMixIn])


    // Ignore loggers
      .addMixIn(classOf[Job], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[OptimizationContext], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[OptimizationUtils], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[SanityChecker], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[CardinalityEstimatorManager], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[CardinalityPusher], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[ChannelConversionGraph], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[LoadProfileEstimators], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[LatentOperatorPruningStrategy], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[PlanEnumeration], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[PlanEnumerator], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[PlanImplementation], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[StageAssignmentTraversal], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[Channel], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[ExecutionPlan], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[PlanTraversal], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[SlotMapping], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[WayangPlan], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[AbstractTopologicalTraversal[_, _]], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[CardinalityBreakpoint], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[CrossPlatformExecutor], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[ExecutorTemplate], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[Junction], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[ExecutionLog], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[AbstractReferenceCountable], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[ReflectionUtils], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[FileSystems], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[HadoopFileSystem], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[LocalFileSystem], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[ObjectFileSource[_]], classOf[IgnoreLoggerMixIn])

    mapper
  }


  def serialize(obj: AnyRef): Array[Byte] = {
    mapper.writeValueAsBytes(obj)
  }

  def serializeAsString(obj: AnyRef): String = {
    mapper.writeValueAsString(obj)
  }

  def deserialize[T: ClassTag](bytes: Array[Byte]): T = {
    val clazz = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    mapper.readValue(bytes, clazz)
  }

  def deserializeFromString[T: ClassTag](string: String): T = {
    val clazz = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    mapper.readValue(string, clazz)
  }

}
