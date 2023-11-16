package org.apache.wayang.api.serialization.customserializers

import com.fasterxml.jackson.core.{JsonGenerator, JsonProcessingException}
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.jsontype.TypeSerializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import org.apache.wayang.api.BlossomContext

import java.io.IOException


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
