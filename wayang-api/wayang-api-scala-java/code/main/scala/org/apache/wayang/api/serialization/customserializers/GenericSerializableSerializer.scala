package org.apache.wayang.api.serialization.customserializers

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.{JsonSerializer, SerializerProvider}

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

class GenericSerializableSerializer[T] extends JsonSerializer[T] {
  override def serialize(value: T, gen: JsonGenerator, serializers: SerializerProvider): Unit = {
    val byteArrayOutputStream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val outputStream: ObjectOutputStream = new ObjectOutputStream(byteArrayOutputStream)
    outputStream.writeObject(value)
    gen.writeBinary(byteArrayOutputStream.toByteArray)
  }
}

