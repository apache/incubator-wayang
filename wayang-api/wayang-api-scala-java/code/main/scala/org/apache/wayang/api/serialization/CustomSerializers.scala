package org.apache.wayang.api.serialization

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser, JsonProcessingException}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonNode, JsonSerializer, SerializerProvider}
import com.fasterxml.jackson.databind.jsontype.{TypeDeserializer, TypeSerializer}
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import org.apache.wayang.api.BlossomContext
import org.apache.wayang.api.serialization.SerializationUtils.mapper
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.function.FunctionDescriptor.SerializablePredicate
import org.apache.wayang.giraph.Giraph
import org.apache.wayang.java.Java
import org.apache.wayang.postgres.Postgres
import org.apache.wayang.spark.Spark
import org.apache.wayang.sqlite3.Sqlite3

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException, ObjectInputStream, ObjectOutputStream}

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

}
