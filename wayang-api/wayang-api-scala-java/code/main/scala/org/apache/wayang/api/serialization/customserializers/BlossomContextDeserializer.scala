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


package org.apache.wayang.api.serialization.customserializers

import com.fasterxml.jackson.core.{JsonParser, JsonProcessingException}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonNode}
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer
import org.apache.wayang.api.BlossomContext
import org.apache.wayang.api.serialization.SerializationUtils.mapper
import org.apache.wayang.core.api.Configuration
import org.apache.wayang.giraph.Giraph
import org.apache.wayang.java.Java
import org.apache.wayang.postgres.Postgres
import org.apache.wayang.spark.Spark
import org.apache.wayang.sqlite3.Sqlite3

import java.io.IOException

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

    // TODO: Add all plugins
    // 3. Add plugins
    val javaPluginName = Java.basicPlugin.getClass.getName
    val sparkPluginName = Spark.basicPlugin.getClass.getName
    val postgresPluginName = Postgres.plugin().getClass.getName
    // val flinkPluginName = Flink.basicPlugin().getClass.getName
    val sqlite3PluginName = Sqlite3.plugin().getClass.getName
    // val giraphPluginName = Giraph.plugin().getClass.getName

    plugins.foreach {
      case pluginName if pluginName == javaPluginName => blossomContext.register(Java.basicPlugin())
      case pluginName if pluginName == sparkPluginName => blossomContext.register(Spark.basicPlugin())
      case pluginName if pluginName == postgresPluginName => blossomContext.register(Postgres.plugin())
      // case pluginName if pluginName == flinkPluginName => blossomContext.register(Flink.basicPlugin())
      case pluginName if pluginName == sqlite3PluginName => blossomContext.register(Sqlite3.plugin())
      // case pluginName if pluginName == giraphPluginName => blossomContext.register(Giraph.plugin())
      case _ => println("Unknown plugin detected")
    }

    blossomContext
  }
}

