package org.apache.wayang.api.serialization.customserializers

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.{JsonSerializer, SerializerProvider}
import org.apache.wayang.core.function.FunctionDescriptor
import org.apache.wayang.core.function.FunctionDescriptor.SerializablePredicate

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

object FunctionDescriptorsSerializers {

  private def serializeFunc(value: AnyRef, gen: JsonGenerator): Unit = {
    val byteArrayOutputStream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val outputStream: ObjectOutputStream = new ObjectOutputStream(byteArrayOutputStream)
    try {
      outputStream.writeObject(value)
      gen.writeBinary(byteArrayOutputStream.toByteArray)
    } finally {
      outputStream.close()
    }
  }

  class SerializablePredicateSerializer extends JsonSerializer[SerializablePredicate[_]] {
    override def serialize(value: SerializablePredicate[_], gen: JsonGenerator, serializers: SerializerProvider): Unit = {
      //      serializeFunc(value, gen)
      val byteArrayOutputStream: ByteArrayOutputStream = new ByteArrayOutputStream()
      val outputStream: ObjectOutputStream = new ObjectOutputStream(byteArrayOutputStream)
      try {
        outputStream.writeObject(value)
        gen.writeBinary(byteArrayOutputStream.toByteArray)
      } finally {
        outputStream.close()
      }
    }
  }

  class SerializableFunctionSerializer extends JsonSerializer[FunctionDescriptor.SerializableFunction[_, _]] {
    override def serialize(value: FunctionDescriptor.SerializableFunction[_, _], gen: JsonGenerator, serializers: SerializerProvider): Unit = {
      //      serializeFunc(value, gen)
      val byteArrayOutputStream: ByteArrayOutputStream = new ByteArrayOutputStream()
      val outputStream: ObjectOutputStream = new ObjectOutputStream(byteArrayOutputStream)
      try {
        outputStream.writeObject(value)
        gen.writeBinary(byteArrayOutputStream.toByteArray)
      } finally {
        outputStream.close()
      }
    }
  }

}
