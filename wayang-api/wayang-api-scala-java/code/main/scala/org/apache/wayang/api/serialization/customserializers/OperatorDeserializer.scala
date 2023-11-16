package org.apache.wayang.api.serialization.customserializers

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonNode}
import org.apache.wayang.api.serialization.SerializationUtils.mapper
import org.apache.wayang.basic.operators.{CollectionSource, FilterOperator, FlatMapOperator, MapOperator, ReduceByOperator, TextFileSource}
import org.apache.wayang.core.function.{FlatMapDescriptor, PredicateDescriptor, ReduceDescriptor, TransformationDescriptor}
import org.apache.wayang.core.plan.wayangplan.Operator
import org.apache.wayang.core.types.DataSetType

import scala.collection.JavaConverters._

class OperatorDeserializer extends JsonDeserializer[Operator] {

  // Define a type for the deserialization function
  private type DeserializerFunction = (JsonParser, JsonNode) => Operator

  // Map type names to deserialization functions
  private val deserializers: Map[String, DeserializerFunction] = Map(
    "TextFileSource" -> deserializeTextFileSource,
    "CollectionSource" -> deserializeCollectionSource,
    "MapOperator" -> deserializeMapOperator,
    "FilterOperator" -> deserializeFilterOperator,
    "FlatMapOperator" -> deserializeFlatMapOperator,
    "ReduceByOperator" -> deserializeReduceByOperator,
  )


  override def deserialize(jp: JsonParser, ctxt: DeserializationContext): Operator = {
    val rootNode: JsonNode = mapper.readTree(jp)
    val typeName = rootNode.get("@type").asText
    deserializers.get(typeName) match {
      case Some(deserializeFunc) => deserializeFunc(jp, rootNode)
      case None => throw new IllegalArgumentException(s"Unknown type: $typeName")
    }
  }


  // Custom deserialization functions for each type
  private def deserializeTextFileSource(jp: JsonParser, rootNode: JsonNode): Operator = {
    val inputUrl = rootNode.get("inputUrl").asText
    new TextFileSource(inputUrl)
  }


  private def deserializeCollectionSource(jp: JsonParser, rootNode: JsonNode): Operator = {
    val collection = mapper.treeToValue(rootNode.get("collection"), classOf[Iterable[_]])
    val t = mapper.treeToValue(rootNode.get("type"), classOf[DataSetType[Any]])
    new CollectionSource(collection.asJavaCollection, t)
  }


  private def deserializeMapOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val functionDescriptor = parseNode(jp, rootNode, "functionDescriptor", new TypeReference[TransformationDescriptor[AnyRef, AnyRef]]() {})
    val operator = new MapOperator(functionDescriptor)
    connectToInputAndReturn(rootNode, operator)
  }

  private def deserializeFlatMapOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val functionDescriptor = parseNode(jp, rootNode, "functionDescriptor", new TypeReference[FlatMapDescriptor[AnyRef, AnyRef]]() {})
    val operator = new FlatMapOperator(functionDescriptor)
    connectToInputAndReturn(rootNode, operator)
  }

  private def deserializeReduceByOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val keyDescriptor = parseNode(jp, rootNode, "keyDescriptor", new TypeReference[TransformationDescriptor[AnyRef, AnyRef]]() {})
    val reduceDescriptor = parseNode(jp, rootNode, "reduceDescriptor", new TypeReference[ReduceDescriptor[AnyRef]]() {})
    val operator = new ReduceByOperator(keyDescriptor, reduceDescriptor)
    connectToInputAndReturn(rootNode, operator)
  }

  private def deserializeFilterOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val predicateDescriptor = parseNode(jp, rootNode, "predicateDescriptor", new TypeReference[PredicateDescriptor[AnyRef]]() {})
    val operator = new FilterOperator(predicateDescriptor)
    connectToInputAndReturn(rootNode, operator)
  }

  private def connectToInputAndReturn(node: JsonNode, operator: Operator): Operator = {
    val inputOperators = getInputOperators(node)
    for ((inputOperator, index) <- inputOperators.zipWithIndex) {
      inputOperator.connectTo(0, operator, index)
    }
    operator
  }

  private def getInputOperators(node: JsonNode): List[Operator] = {

    var inputOperators: List[Operator] = List()

    // Navigate to inputSlots
    val inputSlots = node.get("inputSlots")
    if (inputSlots != null && inputSlots.isArray) {

      // For each input slot
      inputSlots.elements().forEachRemaining { inputSlot =>

        // Access occupant
        val occupant = inputSlot.get("occupant")
        if (occupant != null) {

          // Access owner
          val owner = occupant.get("owner")
          if (owner != null) {

            // Deserialize the nested owner operator and add it into list to be returned
            val jsonParser = owner.traverse(mapper)
            jsonParser.nextToken()
            val inputOperator = mapper.readValue[Operator](jsonParser, classOf[Operator])
            inputOperators = inputOperators :+ inputOperator
          }
        }
      }
    }

    inputOperators
  }

  override def deserializeWithType(p: JsonParser, ctxt: DeserializationContext, typeDeserializer: TypeDeserializer): Operator = {
    deserialize(p, ctxt)
  }

  override def deserializeWithType(p: JsonParser, ctxt: DeserializationContext, typeDeserializer: TypeDeserializer, intoValue: Operator): Operator = {
    deserialize(p, ctxt)
  }
}

