package org.apache.wayang.api.serialization.customserializers

import com.fasterxml.jackson.core.{JsonParser, JsonProcessingException}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonNode}
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer
import org.apache.wayang.api.MultiContext
import org.apache.wayang.api.serialization.SerializationUtils.mapper
import org.apache.wayang.core.api.Configuration
import org.apache.wayang.java.Java
import org.apache.wayang.postgres.Postgres
import org.apache.wayang.spark.Spark
import org.apache.wayang.sqlite3.Sqlite3
import org.apache.wayang.flink.Flink
import org.apache.wayang.hadoop.Hadoop
import org.apache.wayang.graphchi.GraphChi

import java.io.IOException

class MultiContextDeserializer extends JsonDeserializer[MultiContext] {

  override def deserializeWithType(p: JsonParser, ctxt: DeserializationContext, typeDeserializer: TypeDeserializer): AnyRef = {
    this.deserialize(p, ctxt)
  }

  @throws[IOException]
  @throws[JsonProcessingException]
  override def deserialize(jp: JsonParser, ctxt: DeserializationContext): MultiContext = {

    // Deserialize each field of MultiContext separately
    val node: JsonNode = jp.getCodec.readTree(jp)

    val configurationParser: JsonParser = node.get("configuration").traverse(jp.getCodec)
    val configuration: Configuration = mapper.readValue(configurationParser, classOf[Configuration])

    val sinkParser: JsonParser = node.get("sink").traverse(jp.getCodec)
    val sink: Option[MultiContext.UnarySink] = mapper.readValue(sinkParser, new TypeReference[Option[MultiContext.UnarySink]]() {})

    val pluginsParser: JsonParser = node.get("plugins").traverse(jp.getCodec)
    val plugins: List[String] = mapper.readValue(pluginsParser, new TypeReference[List[String]]() {})

    //
    // Create the whole deserialized multi context
    //
    // 1. Add configuration
    val multiContext = new MultiContext(configuration)

    // 2. Add sink
    sink match {
      case Some(MultiContext.TextFileSink(url)) =>
        println(s"It's a TextFileSink with url: $url")
        multiContext.withTextFileSink(url)
      case Some(MultiContext.ObjectFileSink(url)) =>
        println(s"It's an ObjectFileSink with url: $url")
        multiContext.withObjectFileSink(url)
      case None =>
        println("No sink defined")
      case _ =>
        println("Unknown sink type")
    }

    // Add all plugins
    val javaPluginName = Java.basicPlugin().getClass.getName
    val sparkPluginName = Spark.basicPlugin().getClass.getName
    val postgresPluginName = Postgres.plugin().getClass.getName
    val sqlite3PluginName = Sqlite3.plugin().getClass.getName
    val flinkPluginName = Flink.basicPlugin().getClass.getName
    val hadoopPluginName = Hadoop.basicPlugin().getClass.getName
    val graphchiPluginName = GraphChi.basicPlugin().getClass.getName

    plugins.foreach {
      case pluginName if pluginName == javaPluginName => multiContext.register(Java.basicPlugin())
      case pluginName if pluginName == sparkPluginName => multiContext.register(Spark.basicPlugin())
      case pluginName if pluginName == postgresPluginName => multiContext.register(Postgres.plugin())
      case pluginName if pluginName == sqlite3PluginName => multiContext.register(Sqlite3.plugin())
      case pluginName if pluginName == flinkPluginName => multiContext.register(Flink.basicPlugin())
      case pluginName if pluginName == hadoopPluginName => multiContext.register(Hadoop.basicPlugin())
      case pluginName if pluginName == graphchiPluginName => multiContext.register(GraphChi.basicPlugin())
      case _ => println("Unknown plugin detected")
    }

    multiContext
  }
}
