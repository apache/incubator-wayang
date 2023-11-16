package org.apache.wayang.api.serialization.customserializers

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}
import org.apache.wayang.core.function.FunctionDescriptor.{SerializableFunction, SerializablePredicate}

import java.io.{ByteArrayInputStream, ObjectInputStream}

object FunctionDescriptorsDeserializers {

  private def deserializeFunc[T](p: JsonParser): T = {
    val value = p.getBinaryValue
    val byteArrayInputStream = new ByteArrayInputStream(value)
    val inputStream = new ObjectInputStream(byteArrayInputStream)
    try {
      inputStream.readObject().asInstanceOf[T]
    } finally {
      inputStream.close()
    }
  }

  class SerializablePredicateDeserializer[T] extends JsonDeserializer[SerializablePredicate[T]] {
    override def deserialize(p: JsonParser, ctxt: DeserializationContext): SerializablePredicate[T] = {
      //      deserializeFunc[SerializablePredicate[T]](p)
      val value = p.getBinaryValue
      val byteArrayInputStream = new ByteArrayInputStream(value)
      val inputStream = new ObjectInputStream(byteArrayInputStream)
      try {
        inputStream.readObject().asInstanceOf[SerializablePredicate[T]]
      } finally {
        inputStream.close()
      }
    }
  }

  class SerializableFunctionDeserializer[Input, Output] extends JsonDeserializer[SerializableFunction[Input, Output]] {
    override def deserialize(p: JsonParser, ctxt: DeserializationContext): SerializableFunction[Input, Output] = {
      //      deserializeFunc[SerializableFunction[Input, Output]](p)
      val value = p.getBinaryValue
      val byteArrayInputStream = new ByteArrayInputStream(value)
      val inputStream = new ObjectInputStream(byteArrayInputStream)
      try {
        inputStream.readObject().asInstanceOf[SerializableFunction[Input, Output]]
      } finally {
        inputStream.close()
      }
    }
  }

}
