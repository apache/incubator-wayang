package org.apache.wayang.api.serialization

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.JsonNode
import org.apache.wayang.api.serialization.SerializationUtils.mapper

import scala.reflect.ClassTag

package object customserializers {

  def parseNode[T: ClassTag](jp: JsonParser, rootNode: JsonNode, fieldName: String, valueTypeRef: TypeReference[T]): T = {
    val node = rootNode.get(fieldName)
    val parser: JsonParser = node.traverse(jp.getCodec)
    mapper.readValue(parser, valueTypeRef)
  }

}
