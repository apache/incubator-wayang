package org.apache.wayang.api.serialization.customserializers

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonNode}
import org.apache.wayang.api.serialization.SerializationUtils.mapper
import org.apache.wayang.basic.operators._
import org.apache.wayang.core.function._
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
    "MapPartitionsOperator" -> deserializeMapPartitionsOperator,
    "FilterOperator" -> deserializeFilterOperator,
    "FlatMapOperator" -> deserializeFlatMapOperator,
    "SampleOperator" -> deserializeSampleOperator,
    "ReduceByOperator" -> deserializeReduceByOperator,
    "MaterializedGroupByOperator" -> deserializeMaterializedGroupByOperator,
    "GlobalReduceOperator" -> deserializeGlobalReduceOperator,
    "GlobalMaterializedGroupOperator" -> deserializeGlobalMaterializedGroupOperator,
    "GroupByOperator" -> deserializeGroupByOperator,
    "ReduceOperator" -> deserializeReduceOperator,
    "SortOperator" -> deserializeSortOperator,
    "ZipWithIdOperator" -> deserializeZipWithIdOperator,
    "DistinctOperator" -> deserializeDistinctOperator,
    "CountOperator" -> deserializeCountOperator,


/*    "CartesianOperator" -> deserializeCartesianOperator,
    "UnionAllOperator" -> deserializeUnionAllOperator,
    "IntersectOperator" -> deserializeIntersectOperator,
    "JoinOperator" -> deserializeJoinOperator,
    "CoGroupOperator" -> deserializeCoGroupOperator,
    "DoWhileOperator" -> deserializeDoWhileOperator,
    "RepeatOperator" -> deserializeRepeatOperator,
    "LocalCallbackSink" -> deserializeLocalCallbackSink,*/
  )


  override def deserialize(jp: JsonParser, ctxt: DeserializationContext): Operator = {
    val jsonNodeOperator: JsonNode = mapper.readTree(jp)
    val typeName = jsonNodeOperator.get("@type").asText

    deserializers.get(typeName) match {
      case Some(deserializeFunc) =>
        val operator = deserializeFunc(jp, jsonNodeOperator)
        connectToInputOperatorsAndReturn(jsonNodeOperator, operator)
      case None =>
        throw new IllegalArgumentException(s"Unknown type: $typeName")
    }
  }


  //
  // Custom deserialization functions for each type
  //
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
//    val functionDescriptor = parseNode(jp, rootNode, "functionDescriptor", new TypeReference[TransformationDescriptor[AnyRef, AnyRef]]() {})
    val functionDescriptor = mapper.treeToValue(rootNode.get("functionDescriptor"), classOf[TransformationDescriptor[AnyRef, AnyRef]])
    new MapOperator(functionDescriptor)
  }

  private def deserializeMapPartitionsOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
//    val functionDescriptor = parseNode(jp, rootNode, "functionDescriptor", new TypeReference[MapPartitionsDescriptor[AnyRef, AnyRef]]() {})
    val functionDescriptor = mapper.treeToValue(rootNode.get("functionDescriptor"), classOf[MapPartitionsDescriptor[AnyRef, AnyRef]])
    new MapPartitionsOperator(functionDescriptor)
  }

  private def deserializeFilterOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
//    val predicateDescriptor = parseNode(jp, rootNode, "predicateDescriptor", new TypeReference[PredicateDescriptor[AnyRef]]() {})
    val predicateDescriptor = mapper.treeToValue(rootNode.get("predicateDescriptor"), classOf[PredicateDescriptor[AnyRef]])
    new FilterOperator(predicateDescriptor)
  }

  private def deserializeFlatMapOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
//    val functionDescriptor = parseNode(jp, rootNode, "functionDescriptor", new TypeReference[FlatMapDescriptor[AnyRef, AnyRef]]() {})
    val functionDescriptor = mapper.treeToValue(rootNode.get("functionDescriptor"), classOf[FlatMapDescriptor[AnyRef, AnyRef]])
    new FlatMapOperator(functionDescriptor)
  }

  private def deserializeSampleOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    // TODO:
    null
  }

  private def deserializeReduceByOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
//    val keyDescriptor = parseNode(jp, rootNode, "keyDescriptor", new TypeReference[TransformationDescriptor[AnyRef, AnyRef]]() {})
//    val reduceDescriptor = parseNode(jp, rootNode, "reduceDescriptor", new TypeReference[ReduceDescriptor[AnyRef]]() {})
    val keyDescriptor = mapper.treeToValue(rootNode.get("keyDescriptor"), classOf[TransformationDescriptor[AnyRef, AnyRef]])
    val reduceDescriptor = mapper.treeToValue(rootNode.get("reduceDescriptor"), classOf[ReduceDescriptor[AnyRef]])
    new ReduceByOperator(keyDescriptor, reduceDescriptor)
  }

  private def deserializeMaterializedGroupByOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    // val keyDescriptor = parseNode(jp, rootNode, "keyDescriptor", new TypeReference[TransformationDescriptor[AnyRef, AnyRef]]() {})
    val keyDescriptor = mapper.treeToValue(rootNode.get("keyDescriptor"), classOf[TransformationDescriptor[AnyRef, AnyRef]])
    new MaterializedGroupByOperator(keyDescriptor)
  }

  private def deserializeGlobalReduceOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    // val reduceDescriptor = parseNode(jp, rootNode, "reduceDescriptor", new TypeReference[ReduceDescriptor[AnyRef]]() {})
    val reduceDescriptor = mapper.treeToValue(rootNode.get("reduceDescriptor"), classOf[ReduceDescriptor[AnyRef]])
    new GlobalReduceOperator(reduceDescriptor)
  }

  private def deserializeGlobalMaterializedGroupOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val inputType = mapper.treeToValue(rootNode.get("inputType"), classOf[DataSetType[AnyRef]])
    val outputType = mapper.treeToValue(rootNode.get("outputType"), classOf[DataSetType[java.lang.Iterable[AnyRef]]])
    new GlobalMaterializedGroupOperator(inputType, outputType)
  }

  private def deserializeGroupByOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    // val keyDescriptor = parseNode(jp, rootNode, "keyDescriptor", new TypeReference[TransformationDescriptor[AnyRef, AnyRef]]() {})
    val keyDescriptor = mapper.treeToValue(rootNode.get("keyDescriptor"), classOf[TransformationDescriptor[AnyRef, AnyRef]])
    new GroupByOperator(keyDescriptor)
  }
  
  private def deserializeReduceOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    // val reduceDescriptor = parseNode(jp, rootNode, "reduceDescriptor", new TypeReference[ReduceDescriptor[AnyRef]]() {})
    val reduceDescriptor = mapper.treeToValue(rootNode.get("reduceDescriptor"), classOf[ReduceDescriptor[AnyRef]])
    val inputType = mapper.treeToValue(rootNode.get("inputType"), classOf[DataSetType[AnyRef]])
    val outputType = mapper.treeToValue(rootNode.get("outputType"), classOf[DataSetType[AnyRef]])
    new ReduceOperator(reduceDescriptor, inputType, outputType)
  }

  private def deserializeSortOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    // val keyDescriptor = parseNode(jp, rootNode, "keyDescriptor", new TypeReference[TransformationDescriptor[AnyRef, AnyRef]]() {})
    val keyDescriptor = mapper.treeToValue(rootNode.get("keyDescriptor"), classOf[TransformationDescriptor[AnyRef, AnyRef]])
    new SortOperator(keyDescriptor)
  }

  private def deserializeZipWithIdOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val inputType = mapper.treeToValue(rootNode.get("inputType"), classOf[DataSetType[AnyRef]])
    new ZipWithIdOperator(inputType)
  }

  private def deserializeDistinctOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val inputType = mapper.treeToValue(rootNode.get("inputType"), classOf[DataSetType[AnyRef]])
    new DistinctOperator(inputType)
  }

  private def deserializeCountOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val inputType = mapper.treeToValue(rootNode.get("inputType"), classOf[DataSetType[AnyRef]])
    new CountOperator(inputType)
  }


  private def connectToInputOperatorsAndReturn(node: JsonNode, operator: Operator): Operator = {
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

