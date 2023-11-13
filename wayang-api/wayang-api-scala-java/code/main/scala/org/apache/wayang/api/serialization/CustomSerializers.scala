package org.apache.wayang.api.serialization

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser, JsonProcessingException}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonNode, JsonSerializer, SerializerProvider}
import com.fasterxml.jackson.databind.jsontype.{TypeDeserializer, TypeSerializer}
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import org.apache.wayang.api.BlossomContext
import org.apache.wayang.api.serialization.SerializationUtils.mapper
import org.apache.wayang.basic.operators.{FilterOperator, MapOperator, TextFileSource}
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.function.FunctionDescriptor.SerializablePredicate
import org.apache.wayang.core.function.{FunctionDescriptor, PredicateDescriptor, TransformationDescriptor}
import org.apache.wayang.core.plan.wayangplan.Operator
import org.apache.wayang.giraph.Giraph
import org.apache.wayang.java.Java
import org.apache.wayang.postgres.Postgres
import org.apache.wayang.spark.Spark
import org.apache.wayang.sqlite3.Sqlite3

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException, ObjectInputStream, ObjectOutputStream}
import scala.reflect.ClassTag

object CustomSerializers {

  // TODO
  /*
  class WayangContextSerializer extends StdSerializer[WayangContext](classOf[WayangContext]) {

    override def serializeWithType(value: WayangContext, gen: JsonGenerator, serializers: SerializerProvider, typeSer: TypeSerializer): Unit = {
      this.serialize(value, gen, serializers)
    }

    @throws[IOException]
    @throws[JsonProcessingException]
    def serialize(blossomContext: WayangContext, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider): Unit = {
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


  class WayangContextDeserializer extends JsonDeserializer[WayangContext] {

    override def deserializeWithType(p: JsonParser, ctxt: DeserializationContext, typeDeserializer: TypeDeserializer): AnyRef = {
      this.deserialize(p, ctxt)
    }

    @throws[IOException]
    @throws[JsonProcessingException]
    override def deserialize(jp: JsonParser, ctxt: DeserializationContext): WayangContext = {

      // Deserialize each field of BlossomContext separately
      val node: JsonNode = jp.getCodec.readTree(jp)

      //      val configurationNode = node.get("configuration")
      //      println()
      //      println("BlossomContext JSON being deserialized:")
      //      println(configurationNode.toString)
      //      println()

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
      val javaPluginName = Java.basicPlugin.getClass.getName
      val sparkPluginName = Spark.basicPlugin.getClass.getName
      val postgresPluginName = Postgres.plugin().getClass.getName
      val flinkPluginName = Flink.basicPlugin().getClass.getName
      val sqlite3PluginName = Sqlite3.plugin().getClass.getName
      val giraphPluginName = Giraph.plugin().getClass.getName

      plugins.foreach {
        case pluginName if pluginName == javaPluginName => blossomContext.register(Java.basicPlugin())
        case pluginName if pluginName == sparkPluginName => blossomContext.register(Spark.basicPlugin())
        case pluginName if pluginName == postgresPluginName => blossomContext.register(Postgres.plugin())
        case pluginName if pluginName == flinkPluginName => blossomContext.register(Flink.basicPlugin())
        case pluginName if pluginName == sqlite3PluginName => blossomContext.register(Sqlite3.plugin())
        case pluginName if pluginName == giraphPluginName => blossomContext.register(Giraph.plugin())
        case _ => println("Unknown plugin detected")
      }

      blossomContext
    }
  }
*/


  class BlossomContextSerializer extends StdSerializer[BlossomContext](classOf[BlossomContext]) {

    override def serializeWithType(value: BlossomContext, gen: JsonGenerator, serializers: SerializerProvider, typeSer: TypeSerializer): Unit = {
      this.serialize(value, gen, serializers)
    }

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

    override def deserializeWithType(p: JsonParser, ctxt: DeserializationContext, typeDeserializer: TypeDeserializer): AnyRef = {
      this.deserialize(p, ctxt)
    }

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
      val javaPluginName = Java.basicPlugin.getClass.getName
      val sparkPluginName = Spark.basicPlugin.getClass.getName
      val postgresPluginName = Postgres.plugin().getClass.getName
      //      val flinkPluginName = Flink.basicPlugin().getClass.getName
      val sqlite3PluginName = Sqlite3.plugin().getClass.getName
      val giraphPluginName = Giraph.plugin().getClass.getName

      plugins.foreach {
        case pluginName if pluginName == javaPluginName => blossomContext.register(Java.basicPlugin())
        case pluginName if pluginName == sparkPluginName => blossomContext.register(Spark.basicPlugin())
        case pluginName if pluginName == postgresPluginName => blossomContext.register(Postgres.plugin())
        //        case pluginName if pluginName == flinkPluginName => blossomContext.register(Flink.basicPlugin())
        case pluginName if pluginName == sqlite3PluginName => blossomContext.register(Sqlite3.plugin())
        case pluginName if pluginName == giraphPluginName => blossomContext.register(Giraph.plugin())
        case _ => println("Unknown plugin detected")
      }

      blossomContext
    }
  }


  class SerializablePredicateSerializer extends JsonSerializer[SerializablePredicate[_]] {
    @Override
    override def serialize(value: SerializablePredicate[_], gen: JsonGenerator, serializers: SerializerProvider): Unit = {
      val byteArrayOutputStream: ByteArrayOutputStream = new ByteArrayOutputStream()
      val outputStream: ObjectOutputStream = new ObjectOutputStream(byteArrayOutputStream)
      outputStream.writeObject(value)
      gen.writeBinary(byteArrayOutputStream.toByteArray)
    }
  }


  class SerializablePredicateDeserializer[T] extends JsonDeserializer[SerializablePredicate[T]] {
    @Override
    override def deserialize(p: JsonParser, ctxt: DeserializationContext): SerializablePredicate[T] = {
      val value = p.getBinaryValue
      val byteArrayInputStream: ByteArrayInputStream = new ByteArrayInputStream(value)
      val inputStream: ObjectInputStream = new ObjectInputStream(byteArrayInputStream)
      inputStream.readObject().asInstanceOf[SerializablePredicate[T]]
    }
  }


  class SerializableFunctionSerializer extends JsonSerializer[FunctionDescriptor.SerializableFunction[_, _]] {
    @Override
    override def serialize(value: FunctionDescriptor.SerializableFunction[_, _], gen: JsonGenerator, serializers: SerializerProvider): Unit = {
      val byteArrayOutputStream: ByteArrayOutputStream = new ByteArrayOutputStream()
      val outputStream: ObjectOutputStream = new ObjectOutputStream(byteArrayOutputStream)
      outputStream.writeObject(value)
      gen.writeBinary(byteArrayOutputStream.toByteArray)
    }
  }


  class SerializableFunctionDeserializer[Input, Output] extends JsonDeserializer[FunctionDescriptor.SerializableFunction[Input, Output]] {
    @Override
    override def deserialize(p: JsonParser, ctxt: DeserializationContext): FunctionDescriptor.SerializableFunction[Input, Output] = {
      val value = p.getBinaryValue
      val byteArrayInputStream: ByteArrayInputStream = new ByteArrayInputStream(value)
      val inputStream: ObjectInputStream = new ObjectInputStream(byteArrayInputStream)
      inputStream.readObject().asInstanceOf[FunctionDescriptor.SerializableFunction[Input, Output]]
    }
  }


  class OperatorDeserializer extends JsonDeserializer[Operator] {

    override def deserializeWithType(p: JsonParser, ctxt: DeserializationContext, typeDeserializer: TypeDeserializer): Operator = {
      deserialize(p, ctxt)
    }

    override def deserializeWithType(p: JsonParser, ctxt: DeserializationContext, typeDeserializer: TypeDeserializer, intoValue: Operator): Operator = {
      deserialize(p, ctxt)
    }

    // Map of logical type names to their corresponding Scala classes
    private val typeNameToClassMap: Map[String, Class[_ <: Operator]] = Map(
      "TextFileSource" -> classOf[TextFileSource],
      "FilterOperator" -> classOf[FilterOperator[_]],
      "MapOperator" -> classOf[MapOperator[_, _]],
    )

    override def deserialize(jp: JsonParser, ctxt: DeserializationContext): Operator = {

      println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
      val rootNode: JsonNode = mapper.readTree(jp)
      val typeName = rootNode.get("@type").asText

      val concreteClass = typeNameToClassMap.getOrElse(typeName,
        throw new IllegalArgumentException(s"OperatorDeserializer: Unknown type: $typeName"))

      // Custom logic depending on the subtype
      concreteClass match {
        case _ if concreteClass == classOf[TextFileSource] =>
          println("text file source 14141")
          val inputUrl = rootNode.get("inputUrl").asText
          new TextFileSource(inputUrl)

        case _ if concreteClass == classOf[FilterOperator[_]] =>
          println("filter operator 14141")

          val inputOperators = getInputOperators(rootNode, ctxt)
          val predicateDescriptorNode = rootNode.get("predicateDescriptor")
          val predicateDescriptorParser: JsonParser = predicateDescriptorNode.traverse(jp.getCodec)
          val predicateDescriptor: PredicateDescriptor[_] = mapper.readValue(predicateDescriptorParser, new TypeReference[PredicateDescriptor[_]]() {})
          val operator = new FilterOperator(predicateDescriptor)

          for ((inputOperator, index) <- inputOperators.zipWithIndex) {
            inputOperator.connectTo(0, operator, index)
          }

          operator

        case _ if concreteClass == classOf[MapOperator[_, _]] =>
          println("Map operator 14141")

          val inputOperators = getInputOperators(rootNode, ctxt)
          val functionDescriptorNode = rootNode.get("functionDescriptor")
          val functionDescriptorParser: JsonParser = functionDescriptorNode.traverse(jp.getCodec)
          val functionDescriptor: TransformationDescriptor[_, _] = mapper.readValue(functionDescriptorParser, new TypeReference[TransformationDescriptor[_, _]]() {})
          val operator = new MapOperator(functionDescriptor)

          for ((inputOperator, index) <- inputOperators.zipWithIndex) {
            inputOperator.connectTo(0, operator, index)
          }

          operator

        case _ =>
          throw new IllegalArgumentException(s"OperatorDeserializer: Unknown type: $typeName")
      }
    }


    private def getInputOperators(node: JsonNode, ctxt: DeserializationContext): List[Operator] = {

      var operators: List[Operator] = List()

      // Navigate to inputSlots
      val inputSlots = node.get("inputSlots")
      println("x-")
      if (inputSlots != null && inputSlots.isArray) {
        println("x-")
        inputSlots.elements().forEachRemaining { inputSlot =>
          // Access occupant
          println("x-")
          val occupant = inputSlot.get("occupant")
          if (occupant != null) {
            // Access owner
            println("x-")
            val owner = occupant.get("owner")
            if (owner != null) {
              // Deserialize the nested owner operator
              println("x-")
              val jsonParser = owner.traverse(mapper)
              jsonParser.nextToken() // Initialize the JsonParser
              val inputOperator = mapper.readValue[Operator](jsonParser, classOf[Operator]) // Replace Operator with the appropriate class
              operators = operators :+ inputOperator
            }
          }
        }
      }

      operators
    }
  }


  def classToClassTag[T](cls: Class[T]): ClassTag[T] = ClassTag(cls)


  def stringToClass(className: String): Class[_] = {
    if (className.startsWith("scala.Tuple")) {
      // For Scala Tuples, handle them generically
      val tupleSize = className.stripPrefix("scala.Tuple").toInt
      val tupleClassName = s"scala.Tuple$tupleSize"
      Class.forName(tupleClassName)
    } else {
      // For non-tuple types, use Class.forName directly
      Class.forName(className)
    }
  }
}
