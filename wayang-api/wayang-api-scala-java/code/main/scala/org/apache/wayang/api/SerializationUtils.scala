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

package org.apache.wayang.api

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.{JsonAutoDetect, JsonBackReference, JsonCreator, JsonFormat, JsonIdentityInfo, JsonIgnore, JsonManagedReference, JsonProperty, JsonSetter, JsonSubTypes, JsonTypeInfo, JsonTypeName, ObjectIdGenerators, PropertyAccessor}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser, JsonProcessingException}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.node.{ObjectNode, TreeTraversingParser}
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonNode, KeyDeserializer, ObjectMapper, SerializationFeature, SerializerProvider}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.logging.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.wayang.basic.operators.{ObjectFileSource, SampleOperator, TextFileSource}
import org.apache.wayang.core.api.configuration.{CollectionProvider, ConstantValueProvider, ExplicitCollectionProvider, FunctionalCollectionProvider, FunctionalKeyValueProvider, KeyValueProvider, MapBasedKeyValueProvider, ValueProvider}
import org.apache.wayang.core.api.{Configuration, Job, WayangContext}
import org.apache.wayang.core.function.FunctionDescriptor
import org.apache.wayang.core.mapping.{Mapping, OperatorPattern, PlanTransformation}
import org.apache.wayang.core.optimizer.{OptimizationContext, OptimizationUtils, ProbabilisticDoubleInterval, SanityChecker}
import org.apache.wayang.core.optimizer.cardinality.{CardinalityEstimator, CardinalityEstimatorManager, CardinalityPusher}
import org.apache.wayang.core.optimizer.channels.{ChannelConversion, ChannelConversionGraph, DefaultChannelConversion}
import org.apache.wayang.core.optimizer.costs.{LoadProfileEstimator, LoadProfileEstimators, LoadProfileToTimeConverter, TimeToCostConverter}
import org.apache.wayang.core.optimizer.enumeration.{LatentOperatorPruningStrategy, PlanEnumeration, PlanEnumerationPruningStrategy, PlanEnumerator, PlanImplementation, StageAssignmentTraversal}
import org.apache.wayang.core.plan.executionplan.{Channel, ExecutionPlan}
import org.apache.wayang.core.plan.wayangplan.traversal.AbstractTopologicalTraversal
import org.apache.wayang.core.plan.wayangplan.{ExecutionOperator, InputSlot, OperatorBase, OutputSlot, PlanTraversal, SlotMapping, WayangPlan}
import org.apache.wayang.core.platform.{CardinalityBreakpoint, CrossPlatformExecutor, ExecutorTemplate, Junction, Platform}
import org.apache.wayang.core.profiling.{CardinalityRepository, ExecutionLog, InstrumentationStrategy}
import org.apache.wayang.core.util.{AbstractReferenceCountable, ReflectionUtils}
import org.apache.wayang.core.util.fs.{FileSystems, HadoopFileSystem, LocalFileSystem}
import org.apache.wayang.giraph.platform.GiraphPlatform
import org.apache.wayang.java.Java
import org.apache.wayang.java.platform.JavaPlatform
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate
import org.apache.wayang.postgres.platform.PostgresPlatform
import org.apache.wayang.spark.Spark
import org.apache.wayang.spark.platform.SparkPlatform
import org.apache.wayang.sqlite3.platform.Sqlite3Platform

import java.io.IOException
import java.util.function.{BiFunction, ToDoubleFunction}
import scala.reflect.ClassTag

object SerializationUtils {

  class BlossomContextSerializer extends StdSerializer[BlossomContext](classOf[BlossomContext]) {

    @throws[IOException]
    @throws[JsonProcessingException]
    def serialize(blossomContext: BlossomContext, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider): Unit = {
      jsonGenerator.writeStartObject()

      // Use default serialization for the 'configuration' field
      jsonGenerator.writeFieldName("configuration")
      serializerProvider.defaultSerializeValue(blossomContext.getConfiguration, jsonGenerator)

      // Use default serialization for the 'sink' field
      jsonGenerator.writeFieldName("sink")
      blossomContext.getSink match {
        case Some(sink) => serializerProvider.defaultSerializeValue(sink, jsonGenerator)
        case None => jsonGenerator.writeNull()
      }

      // Serialize the plugins list as an array of strings
      jsonGenerator.writeArrayFieldStart("plugins")
      blossomContext.getPlugins.foreach(plugin => jsonGenerator.writeString(plugin))
      jsonGenerator.writeEndArray()

      jsonGenerator.writeEndObject()
    }
  }


  class BlossomContextDeserializer extends JsonDeserializer[BlossomContext] {

    @throws[IOException]
    @throws[JsonProcessingException]
    override def deserialize(jp: JsonParser, ctxt: DeserializationContext): BlossomContext = {

      // Deserialize each field of BlossomContext separately
      val node: JsonNode = jp.getCodec.readTree(jp)

      val configurationParser: JsonParser = node.get("configuration").traverse(jp.getCodec)
      val configuration: Configuration = mapper.readValue(configurationParser, classOf[Configuration])

      val sinkParser: JsonParser = node.get("sink").traverse(jp.getCodec)
      val sink: Option[BlossomContext.UnarySink] = mapper.readValue(sinkParser, new TypeReference[Option[BlossomContext.UnarySink]]() {})

      val pluginsParser: JsonParser = node.get("plugins").traverse(jp.getCodec)
      val plugins: List[String] = mapper.readValue(pluginsParser, new TypeReference[List[String]]() {})

      //
      // Create the whole deserialized blossom context
      //
      // 1. Add configuration
      val blossomContext = new BlossomContext(configuration)

      // 2. Add sink
      sink match {
        case Some(BlossomContext.TextFileSink(url)) =>
          println(s"It's a TextFileSink with url: $url")
          blossomContext.withTextFileSink(url)
        case Some(BlossomContext.ObjectFileSink(url)) =>
          println(s"It's an ObjectFileSink with url: $url")
          blossomContext.withObjectFileSink(url)
        case None =>
          println("No sink defined")
        case _ =>
          println("Unknown sink type")
      }

      // 3. Add plugins
      val javaBasicPluginName = Java.basicPlugin.getClass.getName
      val sparkBasicPluginName = Spark.basicPlugin.getClass.getName

      plugins.foreach {
        case pluginName if pluginName == javaBasicPluginName => blossomContext.register(Java.basicPlugin)
        case pluginName if pluginName == sparkBasicPluginName => blossomContext.register(Spark.basicPlugin)
        case _ => println("Unknown plugin detected")
      }

      blossomContext
    }
  }

  val mapper: ObjectMapper = {
    val mapper = new ObjectMapper()
      .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
      .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
      .enable(SerializationFeature.INDENT_OUTPUT)
      .registerModule(DefaultScalaModule)
      .registerModule(new SimpleModule().addSerializer(classOf[BlossomContext], new BlossomContextSerializer()))
      .registerModule(new SimpleModule().addDeserializer(classOf[BlossomContext], new BlossomContextDeserializer()))

    // OutputSlot[_] is used as a key in a map object, so we need this for deserialization
    //    val module = new SimpleModule().addKeyDeserializer(classOf[OutputSlot[_]], new OutputSlotKeyDeserializer())
    //    mapper.registerModule(module)

    // Register mix-ins during initialization
    mapper
      .addMixIn(classOf[WayangContext], classOf[WayangContextMixIn])
      .addMixIn(classOf[BlossomContext], classOf[IgnoreLoggerMixIn])
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
      .addMixIn(classOf[InputSlot[_]], classOf[InputSlotMixin])
      .addMixIn(classOf[OutputSlot[_]], classOf[OutputSlotMixin])
      .addMixIn(classOf[OperatorBase], classOf[OperatorBaseMixIn])
      .addMixIn(classOf[BlossomContext.UnarySink], classOf[BlossomContextUnarySinkMixIn])
      .addMixIn(classOf[ChannelConversion], classOf[ChannelConversionMixIn])
      .addMixIn(classOf[JavaPlatform], classOf[JavaPlatformMixIn])
      .addMixIn(classOf[SparkPlatform], classOf[SparkPlatformMixIn])
      .addMixIn(classOf[Platform], classOf[PlatformMixIn])
      .addMixIn(classOf[SparkConf], classOf[SparkConfMixIn])


    // Maybe we will need those
    //      .addMixIn(classOf[Job], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[OptimizationContext], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[OptimizationUtils], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[SanityChecker], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[CardinalityEstimatorManager], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[CardinalityPusher], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[ChannelConversionGraph], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[LoadProfileEstimators], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[LatentOperatorPruningStrategy], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[PlanEnumeration], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[PlanEnumerator], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[PlanImplementation], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[StageAssignmentTraversal], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[Channel], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[ExecutionPlan], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[PlanTraversal], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[SlotMapping], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[WayangPlan], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[AbstractTopologicalTraversal[_, _]], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[CardinalityBreakpoint], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[CrossPlatformExecutor], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[ExecutorTemplate], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[Junction], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[ExecutionLog], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[AbstractReferenceCountable], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[ReflectionUtils], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[FileSystems], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[HadoopFileSystem], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[LocalFileSystem], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[SampleOperator[_]], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[ObjectFileSource[_]], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[SampleOperator[_]], classOf[IgnoreLoggerMixIn])
    //      .addMixIn(classOf[TextFileSource], classOf[IgnoreLoggerMixIn])


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

  abstract class SparkConfMixIn {
    @JsonProperty("jars")
    def setJars(jars: Array[String]): Unit
  }

  @JsonAutoDetect(fieldVisibility = Visibility.ANY, getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
  abstract class OperatorBaseMixIn {}

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
    //    @JsonManagedReference
    //    private var configuration: Configuration = _
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
    //    @JsonBackReference
    //    private var parent: FunctionalKeyValueProvider[_, _] = _
    //    @JsonBackReference
    //    private var configuration: Configuration = _
  }

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  abstract class ConstantValueProviderMixIn {
    //    @JsonBackReference
    //    private var configuration: Configuration = _
  }

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  abstract class ExplicitCollectionProviderMixIn {
    @JsonIgnore
    private var logger: Logger = _
    //    @JsonBackReference
    //    private var configuration: Configuration = _
  }

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  abstract class OperatorPatternMixin {
    //    @JsonManagedReference
    //    private var inputSlots: Array[InputSlot[_]] = _
    //    @JsonManagedReference
    //    private var outputSlots: Array[OutputSlot[_]] = _
  }

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  abstract class InputSlotMixin {
    //    @JsonBackReference
    //    private var owner: OperatorPattern[_] = _
  }

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  @JsonFormat(shape = JsonFormat.Shape.OBJECT)  // Serialize as an object when found as a map key
  abstract class OutputSlotMixin {
    //    @JsonBackReference
    //    private var owner: OperatorPattern[_] = _
  }

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  abstract class ConfigurationMixIn {
    //    @JsonIgnore
    //    private var logger: Logger = _

    @JsonIgnore
    private var parent: Configuration = _

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

    //    @JsonBackReference
    //    private var cardinalityEstimatorProvider: FunctionalKeyValueProvider[_, _] = _
    //    @JsonManagedReference
    //    private var udfSelectivityProvider: MapBasedKeyValueProvider[_, _] = _
    //    @JsonManagedReference
    //    private var costSquasherProvider: ConstantValueProvider[_] = _
    //    @JsonManagedReference
    //    private var platformProvider: ExplicitCollectionProvider[_] = _
  }

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[FunctionalKeyValueProvider[_, _]], name = "functionalKeyValueProvider"),
    new JsonSubTypes.Type(value = classOf[MapBasedKeyValueProvider[_, _]], name = "mapBasedKeyValueProvider"
    ))
  )
  abstract class KeyValueProviderMixIn {
    @JsonIgnore
    private var logger: Logger = _
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[BlossomContext.TextFileSink], name = "textFileSink"),
    new JsonSubTypes.Type(value = classOf[BlossomContext.ObjectFileSink], name = "objectFileSink"
    ))
  )
  abstract class BlossomContextUnarySinkMixIn {
  }

  @JsonTypeName("textFileSink")
  abstract class BlossomContextTextFileSink {}

  @JsonTypeName("objectFileSink")
  abstract class ObjectFileSink {}

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[ExplicitCollectionProvider[_]], name = "explicitCollectionProvider"),
    new JsonSubTypes.Type(value = classOf[FunctionalCollectionProvider[_]], name = "functionalCollectionProvider"
    ))
  )
  abstract class CollectionProviderMixIn {
  }

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  abstract class SparkPlatformMixIn {
    @JsonIgnore
    private var logger: Logger = _
  }

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  abstract class JavaPlatformMixIn {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[JavaPlatform], name = "javaPlatform"),
    new JsonSubTypes.Type(value = classOf[SparkPlatform], name = "sparkPlatform"),
    //    new JsonSubTypes.Type(value = classOf[FlinkPlatform], name = "flinkPlatform"),
    new JsonSubTypes.Type(value = classOf[GiraphPlatform], name = "giraphPlatform"),
    //    new JsonSubTypes.Type(value = classOf[GraphChiPlatform], name = "graphChiPlatform"),
    new JsonSubTypes.Type(value = classOf[PostgresPlatform], name = "postgresPlatform"),
    new JsonSubTypes.Type(value = classOf[Sqlite3Platform], name = "sqlite3Platform"),
    //    new JsonSubTypes.Type(value = classOf[HsqldbPlatform], name = "hsqldbPlatform"),
  ))
  abstract class PlatformMixIn {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[DefaultChannelConversion], name = "defaultChannelConversion"),
  ))
  abstract class ChannelConversionMixIn {
  }

  class OutputSlotKeyDeserializer extends KeyDeserializer {
    override def deserializeKey(key: String, ctxt: DeserializationContext): OutputSlot[_] = {
      // Use the internal objectMapper to deserialize the key string into an OutputSlot object
      mapper.readValue(key, classOf[OutputSlot[_]])
    }
  }


  def serialize(obj: AnyRef): Array[Byte] = {
    mapper.writeValueAsBytes(obj)
  }

  def deserialize[T: ClassTag](bytes: Array[Byte]): T = {
    val clazz = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    mapper.readValue(bytes, clazz)
  }

}
