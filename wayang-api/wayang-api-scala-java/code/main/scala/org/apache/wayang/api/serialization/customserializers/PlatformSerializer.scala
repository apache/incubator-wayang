package org.apache.wayang.api.serialization.customserializers

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.{JsonSerializer, SerializerProvider}
import org.apache.wayang.core.platform.Platform

class PlatformSerializer extends JsonSerializer[Platform]{

  def serialize(platform: Platform, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider): Unit = {
    jsonGenerator.writeString(platform.getClass.getName)
  }

}
