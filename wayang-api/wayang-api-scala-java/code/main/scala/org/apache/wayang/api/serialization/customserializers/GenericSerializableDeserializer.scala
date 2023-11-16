package org.apache.wayang.api.serialization.customserializers

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}

import java.io.{ByteArrayInputStream, ObjectInputStream}

class GenericSerializableDeserializer[T] extends JsonDeserializer[T] {

  override def deserialize(p: JsonParser, ctxt: DeserializationContext): T = {
    val value = p.getBinaryValue
    val byteArrayInputStream: ByteArrayInputStream = new ByteArrayInputStream(value)
    val inputStream: ObjectInputStream = new ObjectInputStream(byteArrayInputStream)
    inputStream.readObject().asInstanceOf[T]
  }
}
