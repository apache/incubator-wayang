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

import com.fasterxml.jackson.core.{JsonGenerator, JsonProcessingException}
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.jsontype.TypeSerializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import org.apache.wayang.api.MultiContext

import java.io.IOException


class MultiContextSerializer extends StdSerializer[MultiContext](classOf[MultiContext]) {

  override def serializeWithType(value: MultiContext, gen: JsonGenerator, serializers: SerializerProvider, typeSer: TypeSerializer): Unit = {
    this.serialize(value, gen, serializers)
  }

  @throws[IOException]
  @throws[JsonProcessingException]
  def serialize(multiContext: MultiContext, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider): Unit = {
    jsonGenerator.writeStartObject()

    // Use default serialization for the 'configuration' field
    jsonGenerator.writeFieldName("configuration")
    serializerProvider.defaultSerializeValue(multiContext.getConfiguration, jsonGenerator)

    // Use default serialization for the 'sink' field
    jsonGenerator.writeFieldName("sink")
    multiContext.getSink match {
      case Some(sink) => serializerProvider.defaultSerializeValue(sink, jsonGenerator)
      case None => jsonGenerator.writeNull()
    }

    // Serialize the plugins list as an array of strings
    jsonGenerator.writeArrayFieldStart("plugins")
    multiContext.getPlugins.foreach(plugin => jsonGenerator.writeString(plugin))
    jsonGenerator.writeEndArray()

    // Write the type of the context
    jsonGenerator.writeStringField("@type", "MultiContext")

    jsonGenerator.writeEndObject()
  }
}
