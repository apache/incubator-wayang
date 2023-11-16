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
import org.apache.logging.log4j.Logger
import org.apache.wayang.api.BlossomContext
import org.apache.wayang.api.serialization.customserializers._
import org.apache.wayang.basic.function.ProjectionDescriptor
import org.apache.wayang.basic.operators._
import org.apache.wayang.basic.types.RecordType
import org.apache.wayang.core.api.configuration._
import org.apache.wayang.core.api.{Configuration, Job, WayangContext}
import org.apache.wayang.core.function.FunctionDescriptor.{SerializableBinaryOperator, SerializableConsumer, SerializableFunction, SerializablePredicate}
import org.apache.wayang.core.function._
import org.apache.wayang.core.mapping.{Mapping, OperatorPattern, PlanTransformation}
import org.apache.wayang.core.optimizer.cardinality.{CardinalityEstimate, CardinalityEstimator, CardinalityEstimatorManager, CardinalityPusher}
import org.apache.wayang.core.optimizer.channels.{ChannelConversion, ChannelConversionGraph}
import org.apache.wayang.core.optimizer.costs._
import org.apache.wayang.core.optimizer.enumeration._
import org.apache.wayang.core.optimizer.{OptimizationContext, OptimizationUtils, ProbabilisticDoubleInterval, SanityChecker}
import org.apache.wayang.core.plan.executionplan.{Channel, ExecutionPlan}
import org.apache.wayang.core.plan.wayangplan._
import org.apache.wayang.core.plan.wayangplan.traversal.AbstractTopologicalTraversal
import org.apache.wayang.core.platform._
import org.apache.wayang.core.profiling.{CardinalityRepository, ExecutionLog, InstrumentationStrategy}
import org.apache.wayang.core.types.{BasicDataUnitType, DataSetType, DataUnitGroupType, DataUnitType}
import org.apache.wayang.core.util.fs.{FileSystems, HadoopFileSystem, LocalFileSystem}
import org.apache.wayang.core.util.{AbstractReferenceCountable, ReflectionUtils}

import java.util.function.{BiFunction, ToDoubleBiFunction, ToDoubleFunction}
import scala.reflect.ClassTag

object SerializationUtils {

  val mapper: ObjectMapper = {
    val mapper = new ObjectMapper()
      .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
      .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .enable(SerializationFeature.INDENT_OUTPUT)
      .registerModule(DefaultScalaModule)
      .registerModule(new SimpleModule().addSerializer(classOf[BlossomContext], new BlossomContextSerializer()))
      .registerModule(new SimpleModule().addDeserializer(classOf[BlossomContext], new BlossomContextDeserializer()))
      .registerModule(new SimpleModule().addDeserializer(classOf[Operator], new OperatorDeserializer()))

      .registerModule(new SimpleModule().addSerializer(classOf[SerializablePredicate[_]], new GenericSerializableSerializer[SerializablePredicate[_]]()))
      .registerModule(new SimpleModule().addDeserializer(classOf[SerializablePredicate[_]], new GenericSerializableDeserializer[SerializablePredicate[_]]()))
      .registerModule(new SimpleModule().addSerializer(classOf[SerializableFunction[_, _]], new GenericSerializableSerializer[FunctionDescriptor.SerializableFunction[_, _]]()))
      .registerModule(new SimpleModule().addDeserializer(classOf[SerializableFunction[_, _]], new GenericSerializableDeserializer[FunctionDescriptor.SerializableFunction[_, _]]()))
      .registerModule(new SimpleModule().addSerializer(classOf[SerializableBinaryOperator[_]], new GenericSerializableSerializer[SerializableBinaryOperator[_]]()))
      .registerModule(new SimpleModule().addDeserializer(classOf[SerializableBinaryOperator[_]], new GenericSerializableDeserializer[SerializableBinaryOperator[_]]()))
      .registerModule(new SimpleModule().addSerializer(classOf[SerializableConsumer[_]], new GenericSerializableSerializer[SerializableConsumer[_]]()))
      .registerModule(new SimpleModule().addDeserializer(classOf[SerializableConsumer[_]], new GenericSerializableDeserializer[SerializableConsumer[_]]()))

    //      .registerModule(new SimpleModule().addSerializer(classOf[SerializablePredicate[_]], new FunctionDescriptorsSerializers.SerializablePredicateSerializer()))
    //      .registerModule(new SimpleModule().addDeserializer(classOf[SerializablePredicate[_]], new FunctionDescriptorsDeserializers.SerializablePredicateDeserializer[AnyRef]()))
    //      .registerModule(new SimpleModule().addSerializer(classOf[SerializableFunction[_, _]], new FunctionDescriptorsSerializers.SerializableFunctionSerializer()))
    //      .registerModule(new SimpleModule().addDeserializer(classOf[SerializableFunction[_, _]], new FunctionDescriptorsDeserializers.SerializableFunctionDeserializer[AnyRef, AnyRef]()))

    // Register mix-ins during initialization
    mapper
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
      .addMixIn(classOf[InputSlot[_]], classOf[InputSlotMixIn[_]])
      .addMixIn(classOf[OutputSlot[_]], classOf[OutputSlotMixIn[_]])
      .addMixIn(classOf[OperatorBase], classOf[OperatorBaseMixIn])
      .addMixIn(classOf[BlossomContext.UnarySink], classOf[BlossomContextUnarySinkMixIn])
      .addMixIn(classOf[ElementaryOperator], classOf[ElementaryOperatorMixIn])
      .addMixIn(classOf[ActualOperator], classOf[ActualOperatorMixIn])
      .addMixIn(classOf[Operator], classOf[OperatorMixIn])
      .addMixIn(classOf[FilterOperator[_]], classOf[FilterOperatorMixIn[_]])
      .addMixIn(classOf[MapOperator[_, _]], classOf[MapOperatorMixIn[_, _]])
      .addMixIn(classOf[PredicateDescriptor[_]], classOf[PredicateDescriptorMixIn[_]])
      .addMixIn(classOf[TransformationDescriptor[_, _]], classOf[TransformationDescriptorMixIn[_, _]])
      .addMixIn(classOf[BasicDataUnitType[_]], classOf[BasicDataUnitTypeMixIn[_]])
      .addMixIn(classOf[ProbabilisticDoubleInterval], classOf[ProbabilisticDoubleIntervalMixIn])
      .addMixIn(classOf[LoadProfileEstimator], classOf[LoadProfileEstimatorMixIn])
      .addMixIn(classOf[FunctionDescriptor], classOf[FunctionDescriptorMixIn])
      //      .addMixIn(classOf[ConstantLoadProfileEstimator], classOf[ConstantLoadProfileEstimatorMixIn])
      .addMixIn(classOf[NestableLoadProfileEstimator], classOf[NestableLoadProfileEstimatorMixIn])
      .addMixIn(classOf[LoadEstimator], classOf[LoadEstimatorMixIn])
      .addMixIn(classOf[DefaultLoadEstimator], classOf[DefaultLoadEstimatorMixIn])
      .addMixIn(classOf[CardinalityEstimate], classOf[CardinalityEstimateMixIn])
      .addMixIn(classOf[DataSetType[_]], classOf[DataSetTypeMixIn[_]])
      .addMixIn(classOf[DataUnitType[_]], classOf[DataUnitTypeMixIn])

      //      .addMixIn(classOf[ChannelConversion], classOf[ChannelConversionMixIn])
      //      .addMixIn(classOf[JavaPlatform], classOf[JavaPlatformMixIn])
      //      .addMixIn(classOf[SparkPlatform], classOf[SparkPlatformMixIn])
      //      .addMixIn(classOf[Platform], classOf[PlatformMixIn])
      //      .addMixIn(classOf[SparkConf], classOf[SparkConfMixIn])

      // Ignore damn loggers
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
      .addMixIn(classOf[SampleOperator[_]], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[ObjectFileSource[_]], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[SampleOperator[_]], classOf[IgnoreLoggerMixIn])
      .addMixIn(classOf[TextFileSource], classOf[TextFileSourceMixIn])
      .addMixIn(classOf[UnarySource[_]], classOf[UnarySourceMixIn[_]])
      .addMixIn(classOf[UnarySink[_]], classOf[UnarySinkMixIn[_]])
      .addMixIn(classOf[UnaryToUnaryOperator[_, _]], classOf[UnaryToUnaryOperatorMixIn[_, _]])
      .addMixIn(classOf[BinaryToUnaryOperator[_, _, _]], classOf[BinaryToUnaryOperatorMixIn[_, _, _]])

    // IntelliJ can't find imports so probably we won't need those
    //        .addMixIn(classOf[FlinkPlatform], classOf[IgnoreLoggerMixIn])
    //          .addMixIn(classOf[JdbcExecutor], classOf[IgnoreLoggerMixIn])
    //          .addMixIn(classOf[SparkListener], classOf[IgnoreLoggerMixIn])
    //          .addMixIn(classOf[SparkObjectFileSource], classOf[IgnoreLoggerMixIn])
    //          .addMixIn(classOf[OperatorProfiler], classOf[IgnoreLoggerMixIn])
    //          .addMixIn(classOf[DynamicLoadProfileEstimators], classOf[IgnoreLoggerMixIn])
    //          .addMixIn(classOf[GeneticOptimizerApp], classOf[IgnoreLoggerMixIn])
    //          .addMixIn(classOf[LogEvaluator], classOf[IgnoreLoggerMixIn])
    //          .addMixIn(classOf[SparkOperatorProfiler], classOf[IgnoreLoggerMixIn])
    //          .addMixIn(classOf[RrdAccessor], classOf[IgnoreLoggerMixIn])
    //          .addMixIn(classOf[GiraphPageRankOperator], classOf[IgnoreLoggerMixIn])
    //          .addMixIn(classOf[GraphChiPageRankOperator], classOf[IgnoreLoggerMixIn])

    mapper
  }

  abstract class IgnoreLoggerMixIn {
    @JsonIgnore
    private var logger: Logger = _
  }

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[OperatorBase], name = "OperatorBase"),
    new JsonSubTypes.Type(value = classOf[ActualOperator], name = "ActualOperator"),
    new JsonSubTypes.Type(value = classOf[CompositeOperator], name = "CompositeOperator"),
    new JsonSubTypes.Type(value = classOf[LoopHeadOperator], name = "LoopHeadOperator"),
  ))
  abstract class OperatorMixIn {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[ElementaryOperator], name = "ElementaryOperator"),
    new JsonSubTypes.Type(value = classOf[Subplan], name = "Subplan"),
  ))
  abstract class ActualOperatorMixIn {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[UnarySource[_]], name = "UnarySource"),
    new JsonSubTypes.Type(value = classOf[UnarySink[_]], name = "UnarySink"),
    new JsonSubTypes.Type(value = classOf[UnaryToUnaryOperator[_, _]], name = "UnaryToUnaryOperator"),
    new JsonSubTypes.Type(value = classOf[BinaryToUnaryOperator[_, _, _]], name = "BinaryToUnaryOperator"),
    new JsonSubTypes.Type(value = classOf[DoWhileOperator[_, _]], name = "DoWhileOperator"),
    new JsonSubTypes.Type(value = classOf[RepeatOperator[_]], name = "RepeatOperator"),
  ))
  abstract class OperatorBaseMixIn {
    @JsonIgnore
    def getOriginal(): ExecutionOperator

    @JsonIgnore
    private var original: ExecutionOperator = _

  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[UnarySource[_]], name = "UnarySource"),
    new JsonSubTypes.Type(value = classOf[UnarySink[_]], name = "UnarySink"),
    new JsonSubTypes.Type(value = classOf[UnaryToUnaryOperator[_, _]], name = "UnaryToUnaryOperator"),
    new JsonSubTypes.Type(value = classOf[BinaryToUnaryOperator[_, _, _]], name = "BinaryToUnaryOperator"),
    new JsonSubTypes.Type(value = classOf[DoWhileOperator[_, _]], name = "DoWhileOperator"),
    new JsonSubTypes.Type(value = classOf[RepeatOperator[_]], name = "RepeatOperator"),
  ))
  abstract class ElementaryOperatorMixIn {
  }

  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[TextFileSource], name = "TextFileSource"),
    new JsonSubTypes.Type(value = classOf[CollectionSource[_]], name = "CollectionSource"),
  ))
  abstract class UnarySourceMixIn[T] {
  }

  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[LocalCallbackSink[_]], name = "LocalCallbackSink"),
  ))
  abstract class UnarySinkMixIn[T] {
  }

  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[MapOperator[_, _]], name = "MapOperator"),
    new JsonSubTypes.Type(value = classOf[MapPartitionsOperator[_, _]], name = "MapPartitionsOperator"),
    new JsonSubTypes.Type(value = classOf[FilterOperator[_]], name = "FilterOperator"),
    new JsonSubTypes.Type(value = classOf[FlatMapOperator[_, _]], name = "FlatMapOperator"),
    new JsonSubTypes.Type(value = classOf[SampleOperator[_]], name = "SampleOperator"),
    new JsonSubTypes.Type(value = classOf[ReduceByOperator[_, _]], name = "ReduceByOperator"),
    new JsonSubTypes.Type(value = classOf[GroupByOperator[_, _]], name = "GroupByOperator"),
    new JsonSubTypes.Type(value = classOf[ReduceOperator[_]], name = "ReduceOperator"),
    new JsonSubTypes.Type(value = classOf[SortOperator[_, _]], name = "SortOperator"),
    new JsonSubTypes.Type(value = classOf[CartesianOperator[_, _]], name = "CartesianOperator"),
    new JsonSubTypes.Type(value = classOf[ZipWithIdOperator[_]], name = "ZipWithIdOperator"),
    new JsonSubTypes.Type(value = classOf[DistinctOperator[_]], name = "DistinctOperator"),
    new JsonSubTypes.Type(value = classOf[CountOperator[_]], name = "CountOperator"),
  ))
  abstract class UnaryToUnaryOperatorMixIn[InputType, OutputType] {
  }

    @JsonSubTypes(Array(
      new JsonSubTypes.Type(value = classOf[UnionAllOperator[_]], name = "UnionAllOperator"),
      new JsonSubTypes.Type(value = classOf[IntersectOperator[_]], name = "IntersectOperator"),
      new JsonSubTypes.Type(value = classOf[JoinOperator[_, _, _]], name = "JoinOperator"),
      new JsonSubTypes.Type(value = classOf[CoGroupOperator[_, _, _]], name = "CoGroupOperator"),
    ))
  abstract class BinaryToUnaryOperatorMixIn[InputType0, InputType1, OutputType] {
  }


  abstract class TextFileSourceMixIn {

    @JsonIgnore
    private var logger: Logger = _

    @JsonCreator
    def this(@JsonProperty("inputUrl") inputUrl: String,
             @JsonProperty("encoding") encoding: String) = {
      this()
    }
  }

  @JsonAutoDetect(fieldVisibility = Visibility.ANY, getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
  abstract class FilterOperatorMixIn[Type] {
    @JsonCreator
    def this(@JsonProperty("predicateDescriptor") predicateDescriptor: PredicateDescriptor[Type],
             @JsonProperty("type") `type`: DataSetType[Type]) = {
      this()
    }
  }

  abstract class MapOperatorMixIn[InputType, OutputType] {
    @JsonCreator
    def this(@JsonProperty("functionDescriptor") functionDescriptor: TransformationDescriptor[InputType, OutputType],
             @JsonProperty("inputType") inputType: DataSetType[InputType],
             @JsonProperty("outputType") outputType: DataSetType[OutputType]) = {
      this()
    }
  }


  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonAutoDetect(fieldVisibility = Visibility.ANY, getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[InputSlot[_]], name = "InputSlot"),
    new JsonSubTypes.Type(value = classOf[OutputSlot[_]], name = "OutputSlot"),
  ))
  abstract class SlotMixIn[T] {

    //    @JsonCreator
    //    def this(@JsonProperty("name") name: String,
    //             @JsonProperty("owner") owner: Operator,
    //             @JsonProperty("type") `type`: DataSetType[T]) = {
    //      this()
    //    }
  }

  //  @JsonAutoDetect(fieldVisibility = Visibility.ANY, getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
  abstract class InputSlotMixIn[T] {
    @JsonCreator
    def this(@JsonProperty("name") name: String,
             @JsonProperty("owner") owner: Operator,
             @JsonProperty("isBroadcast") isBroadcast: Boolean,
             @JsonProperty("type") `type`: DataSetType[T]) = {
      this()
    }
  }

  //  @JsonAutoDetect(fieldVisibility = Visibility.ANY, getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
  abstract class OutputSlotMixIn[T] {
    @JsonCreator
    def this(@JsonProperty("name") name: String,
             @JsonProperty("owner") owner: Operator,
             @JsonProperty("type") `type`: DataSetType[T]) = {
      this()
    }
  }

  abstract class WayangContextMixIn {
    @JsonIgnore
    private var logger: Logger = _

    // TODO: Is this okay?
    @JsonIgnore
    private var cardinalityRepository: CardinalityRepository = _
  }

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  abstract class FunctionalKeyValueProviderMixIn[Key, Value] {
    @JsonCreator
    def this(@JsonProperty("parent") parent: KeyValueProvider[Key, Value],
             @JsonProperty("configuration") configuration: Configuration,
             @JsonProperty("providerFunction") providerFunction: BiFunction[Key, KeyValueProvider[Key, Value], Value]) = {
      this()
    }
  }

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  abstract class MapBasedKeyValueProviderMixIn[Key, Value] {
    @JsonSetter("storedValues")
    private def setStoredValues(storedValues: Map[Key, Value]): Unit = {}

    @JsonCreator
    def this(@JsonProperty("parent") parent: KeyValueProvider[Key, Value],
             @JsonProperty("configuration") configuration: Configuration,
             @JsonProperty("isCaching") isCaching: Boolean) = {
      this()
    }
  }

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  abstract class ConstantValueProviderMixIn {
  }

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  abstract class ExplicitCollectionProviderMixIn {
    @JsonIgnore
    private var logger: Logger = _
  }

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  abstract class OperatorPatternMixin {
  }

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  abstract class ConfigurationMixIn {

    @JsonIgnore
    private var cardinalityEstimatorProvider: KeyValueProvider[OutputSlot[_], CardinalityEstimator] = _

    @JsonIgnore
    private var udfSelectivityProvider: KeyValueProvider[FunctionDescriptor, ProbabilisticDoubleInterval] = _

    @JsonIgnore
    private var operatorLoadProfileEstimatorProvider: KeyValueProvider[ExecutionOperator, LoadProfileEstimator] = _

    @JsonIgnore
    private var functionLoadProfileEstimatorProvider: KeyValueProvider[FunctionDescriptor, LoadProfileEstimator] = _

    @JsonIgnore
    private var loadProfileEstimatorCache: MapBasedKeyValueProvider[String, LoadProfileEstimator] = _

    @JsonIgnore
    private var loadProfileToTimeConverterProvider: KeyValueProvider[Platform, LoadProfileToTimeConverter] = _

    @JsonIgnore
    private var timeToCostConverterProvider: KeyValueProvider[Platform, TimeToCostConverter] = _

    @JsonIgnore
    private var costSquasherProvider: ValueProvider[ToDoubleFunction[ProbabilisticDoubleInterval]] = _

    @JsonIgnore
    private var platformStartUpTimeProvider: KeyValueProvider[Platform, Long] = _

    @JsonIgnore
    private var platformProvider: ExplicitCollectionProvider[Platform] = _

    @JsonIgnore
    private var mappingProvider: ExplicitCollectionProvider[Mapping] = _

    @JsonIgnore
    private var channelConversionProvider: ExplicitCollectionProvider[ChannelConversion] = _

    @JsonIgnore
    private var pruningStrategyClassProvider: CollectionProvider[Class[PlanEnumerationPruningStrategy]] = _

    @JsonIgnore
    private var instrumentationStrategyProvider: ValueProvider[InstrumentationStrategy] = _
  }

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[FunctionalKeyValueProvider[_, _]], name = "FunctionalKeyValueProvider"),
    new JsonSubTypes.Type(value = classOf[MapBasedKeyValueProvider[_, _]], name = "MapBasedKeyValueProvider"
    ))
  )
  abstract class KeyValueProviderMixIn {
    @JsonIgnore
    private var logger: Logger = _
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[BlossomContext.TextFileSink], name = "BlossomContextTextFileSink"),
    new JsonSubTypes.Type(value = classOf[BlossomContext.ObjectFileSink], name = "BlossomContextObjectFileSink"
    ))
  )
  abstract class BlossomContextUnarySinkMixIn {}

  //  @JsonTypeName("textFileSink")
  //  abstract class BlossomContextTextFileSink {}
  //
  //  @JsonTypeName("objectFileSink")
  //  abstract class ObjectFileSinkMixIn {}

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[ExplicitCollectionProvider[_]], name = "ExplicitCollectionProvider"),
    new JsonSubTypes.Type(value = classOf[FunctionalCollectionProvider[_]], name = "FunctionalCollectionProvider"
    ))
  )
  abstract class CollectionProviderMixIn {
  }

  abstract class ProbabilisticDoubleIntervalMixIn {
    @JsonCreator
    def this(@JsonProperty("lowerEstimate") lowerEstimate: Double,
             @JsonProperty("upperEstimate") upperEstimate: Double,
             @JsonProperty("correctnessProb") correctnessProb: Double,
             @JsonProperty("isOverride") isOverride: Boolean) = {
      this()
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[ConstantLoadProfileEstimator], name = "ConstantLoadProfileEstimator"),
    new JsonSubTypes.Type(value = classOf[NestableLoadProfileEstimator], name = "NestableLoadProfileEstimator"),
  ))
  abstract class LoadProfileEstimatorMixIn {
  }

  //  @JsonTypeName("constantLoadProfileEstimator")
  //  abstract class ConstantLoadProfileEstimatorMixIn {}
  //
  @JsonTypeName("nestableLoadProfileEstimator")
  abstract class NestableLoadProfileEstimatorMixIn {
    @JsonCreator
    def this (@JsonProperty("cpuLoadEstimator") cpuLoadEstimator : LoadEstimator,
              @JsonProperty("ramLoadEstimator") ramLoadEstimator: LoadEstimator,
              @JsonProperty("diskLoadEstimator") diskLoadEstimator: LoadEstimator,
              @JsonProperty("networkLoadEstimator") networkLoadEstimator: LoadEstimator,
              @JsonProperty("resourceUtilizationEstimator") resourceUtilizationEstimator: ToDoubleBiFunction[Array[Long], Array[Long]],
              @JsonProperty("overheadMillis") overheadMillis: Long,
              @JsonProperty("configurationKey") configurationKey: String
             ) = {
      this()
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[ProjectionDescriptor[_, _]], name = "ProjectionDescriptor"),
    new JsonSubTypes.Type(value = classOf[AggregationDescriptor[_, _]], name = "AggregationDescriptor"),
    new JsonSubTypes.Type(value = classOf[ConsumerDescriptor[_]], name = "ConsumerDescriptor"),
    new JsonSubTypes.Type(value = classOf[FlatMapDescriptor[_, _]], name = "FlatMapDescriptor"),
    new JsonSubTypes.Type(value = classOf[MapPartitionsDescriptor[_, _]], name = "MapPartitionsDescriptor"),
    new JsonSubTypes.Type(value = classOf[PredicateDescriptor[_]], name = "PredicateDescriptor"),
    new JsonSubTypes.Type(value = classOf[ReduceDescriptor[_]], name = "ReduceDescriptor"),
    new JsonSubTypes.Type(value = classOf[TransformationDescriptor[_, _]], name = "TransformationDescriptor"),
  ))
  abstract class FunctionDescriptorMixIn {
    @JsonIgnore
    private var loadProfileEstimator: LoadProfileEstimator = _
  }

  @JsonAutoDetect(fieldVisibility = Visibility.ANY, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE)
  abstract class PredicateDescriptorMixIn[Input] {
    @JsonCreator
    def this(@JsonProperty("javaImplementation") javaImplementation: SerializablePredicate[Input],
             @JsonProperty("inputType") inputType: BasicDataUnitType[Input],
             @JsonProperty("selectivity") selectivity: ProbabilisticDoubleInterval,
             @JsonProperty("loadProfileEstimator") loadProfileEstimator: LoadProfileEstimator) = {
      this()
    }
  }

  @JsonAutoDetect(fieldVisibility = Visibility.ANY, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE)
  abstract class TransformationDescriptorMixIn[Input, Output] {
    @JsonCreator def this(@JsonProperty("javaImplementation") javaImplementation: FunctionDescriptor.SerializableFunction[Input, Output],
                          @JsonProperty("inputType") inputType: BasicDataUnitType[Input],
                          @JsonProperty("outputType") outputType: BasicDataUnitType[Output],
                          @JsonProperty("loadProfileEstimator") loadProfileEstimator: LoadProfileEstimator) = {
      this()
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[DefaultLoadEstimator], name = "DefaultLoadEstimator"),
    new JsonSubTypes.Type(value = classOf[IntervalLoadEstimator], name = "IntervalLoadEstimator"),
  ))
  abstract class LoadEstimatorMixIn {
  }

  abstract class DefaultLoadEstimatorMixIn {
    @JsonCreator
    def this(@JsonProperty("numInputs") numInputs: Int,
             @JsonProperty("numOutputs") numOutputs: Int,
             @JsonProperty("correctnessProbability") correctnessProbability: Double,
             @JsonProperty("nullCardinalityReplacement") nullCardinalityReplacement: CardinalityEstimate,
             @JsonProperty("singlePointFunction") singlePointFunction: LoadEstimator.SinglePointEstimationFunction) = {
      this()
    }
  }

  abstract class CardinalityEstimateMixIn {
    @JsonCreator
    def this(@JsonProperty("lowerEstimate") lowerEstimate: Long,
             @JsonProperty("upperEstimate") upperEstimate: Long,
             @JsonProperty("correctnessProb") correctnessProb: Double,
             @JsonProperty("isOverride") isOverride: Boolean) = {
      this()
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[RecordType], name = "RecordType"),
  ))
  abstract class BasicDataUnitTypeMixIn[T] {
    @JsonCreator
    def this(@JsonProperty("typeClass") typeClass: Class[T]) = {
      this()
    }
  }

  abstract class DataSetTypeMixIn[T] {
    @JsonCreator
    def this(@JsonProperty("dataUnitType") dataUnitType: DataUnitType[T]) = {
      this()
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[BasicDataUnitType[_]], name = "BasicDataUnitType"),
    new JsonSubTypes.Type(value = classOf[DataUnitGroupType[_]], name = "DataUnitGroupType"),
  ))
  abstract class DataUnitTypeMixIn {
  }

  //  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  //  abstract class SparkPlatformMixIn {
  //    @JsonIgnore
  //    private var logger: Logger = _
  //  }
  //
  //  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  //  abstract class JavaPlatformMixIn {
  //  }

  //  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  //  @JsonSubTypes(Array(
  //    new JsonSubTypes.Type(value = classOf[JavaPlatform], name = "javaPlatform"),
  //    new JsonSubTypes.Type(value = classOf[SparkPlatform], name = "sparkPlatform"),
  //    //    new JsonSubTypes.Type(value = classOf[FlinkPlatform], name = "flinkPlatform"),
  //    new JsonSubTypes.Type(value = classOf[GiraphPlatform], name = "giraphPlatform"),
  //    //    new JsonSubTypes.Type(value = classOf[GraphChiPlatform], name = "graphChiPlatform"),
  //    new JsonSubTypes.Type(value = classOf[PostgresPlatform], name = "postgresPlatform"),
  //    new JsonSubTypes.Type(value = classOf[Sqlite3Platform], name = "sqlite3Platform"),
  //    //    new JsonSubTypes.Type(value = classOf[HsqldbPlatform], name = "hsqldbPlatform"),
  //  ))
  //  abstract class PlatformMixIn {
  //  }
  //
  //  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  //  @JsonSubTypes(Array(
  //    new JsonSubTypes.Type(value = classOf[DefaultChannelConversion], name = "defaultChannelConversion"),
  //  ))
  //  abstract class ChannelConversionMixIn {
  //  }

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
