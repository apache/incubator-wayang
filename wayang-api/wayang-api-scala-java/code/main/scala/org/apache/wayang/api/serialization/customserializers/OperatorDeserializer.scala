package org.apache.wayang.api.serialization.customserializers

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonNode}
import org.apache.wayang.api.serialization.SerializationUtils.mapper
import org.apache.wayang.api.serialization.customserializers.OperatorDeserializer.{inputSlotOwnerIdMap, outputSlotOwnerIdMap}
import org.apache.wayang.basic.operators._
import org.apache.wayang.core.api.exception.WayangException
import org.apache.wayang.core.function.FunctionDescriptor.{SerializableIntUnaryOperator, SerializableLongUnaryOperator}
import org.apache.wayang.core.function._
import org.apache.wayang.core.plan.wayangplan.Operator
import org.apache.wayang.core.types.DataSetType

import scala.collection.JavaConverters._
import scala.collection.mutable

class OperatorDeserializer extends JsonDeserializer[Operator] {

  // Define a type for the deserialization function
  private type DeserializerFunction = (JsonParser, JsonNode) => Operator

  // Map type names to deserialization functions
  private val deserializers: Map[String, DeserializerFunction] = Map(

    // Source
    "TextFileSource" -> deserializeTextFileSource,
    "CollectionSource" -> deserializeCollectionSource,

    // Unary
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

    // Binary
    "CartesianOperator" -> deserializeCartesianOperator,
    "UnionAllOperator" -> deserializeUnionAllOperator,
    "IntersectOperator" -> deserializeIntersectOperator,
    "JoinOperator" -> deserializeJoinOperator,
    "CoGroupOperator" -> deserializeCoGroupOperator,

    // Loop
    "DoWhileOperator" -> deserializeDoWhileOperator,
    "RepeatOperator" -> deserializeRepeatOperator,

/*
    "LocalCallbackSink" -> deserializeLocalCallbackSink,
 */
  )


  override def deserialize(jp: JsonParser, ctxt: DeserializationContext): Operator = {
    val objectIdMap = OperatorDeserializer.operatorIdMap.get()
    val jsonNodeOperator: JsonNode = mapper.readTree(jp)

    if (jsonNodeOperator.get("@type") == null) {
      objectIdMap.get(jsonNodeOperator.asLong()) match {
        case Some(operator) => return operator
        case None => throw new WayangException(s"Can't deserialize operator with id ${jsonNodeOperator.asLong()}")
      }
    }
    val typeName = jsonNodeOperator.get("@type").asText
    val id = jsonNodeOperator.get("@id").asLong
    // println(s"Processing operator $typeName")

    deserializers.get(typeName) match {
      case Some(deserializeFunc) =>
        val operator = deserializeFunc(jp, jsonNodeOperator)
        // println(s"\tStoring $typeName with id ${id}")
        objectIdMap.put(id, operator)
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
    val functionDescriptor = mapper.treeToValue(rootNode.get("functionDescriptor"), classOf[TransformationDescriptor[AnyRef, AnyRef]])
    new MapOperator(functionDescriptor)
  }

  private def deserializeMapPartitionsOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val functionDescriptor = mapper.treeToValue(rootNode.get("functionDescriptor"), classOf[MapPartitionsDescriptor[AnyRef, AnyRef]])
    new MapPartitionsOperator(functionDescriptor)
  }

  private def deserializeFilterOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val predicateDescriptor = mapper.treeToValue(rootNode.get("predicateDescriptor"), classOf[PredicateDescriptor[AnyRef]])
    new FilterOperator(predicateDescriptor)
  }

  private def deserializeFlatMapOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val functionDescriptor = mapper.treeToValue(rootNode.get("functionDescriptor"), classOf[FlatMapDescriptor[AnyRef, AnyRef]])
    new FlatMapOperator(functionDescriptor)
  }

  private def deserializeSampleOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val sampleSizeFunction = mapper.treeToValue(rootNode.get("sampleSizeFunction"), classOf[SerializableIntUnaryOperator])
    val typeValue = mapper.treeToValue(rootNode.get("type"), classOf[DataSetType[AnyRef]])
    val sampleMethod = mapper.treeToValue(rootNode.get("sampleMethod"), classOf[SampleOperator.Methods])
    val seedFunction = mapper.treeToValue(rootNode.get("seedFunction"), classOf[SerializableLongUnaryOperator])
    new SampleOperator(sampleSizeFunction, typeValue, sampleMethod, seedFunction)
  }

  private def deserializeReduceByOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val keyDescriptor = mapper.treeToValue(rootNode.get("keyDescriptor"), classOf[TransformationDescriptor[AnyRef, AnyRef]])
    val reduceDescriptor = mapper.treeToValue(rootNode.get("reduceDescriptor"), classOf[ReduceDescriptor[AnyRef]])
    new ReduceByOperator(keyDescriptor, reduceDescriptor)
  }

  private def deserializeMaterializedGroupByOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val keyDescriptor = mapper.treeToValue(rootNode.get("keyDescriptor"), classOf[TransformationDescriptor[AnyRef, AnyRef]])
    new MaterializedGroupByOperator(keyDescriptor)
  }

  private def deserializeGlobalReduceOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val reduceDescriptor = mapper.treeToValue(rootNode.get("reduceDescriptor"), classOf[ReduceDescriptor[AnyRef]])
    new GlobalReduceOperator(reduceDescriptor)
  }

  private def deserializeGlobalMaterializedGroupOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val inputType = mapper.treeToValue(rootNode.get("inputType"), classOf[DataSetType[AnyRef]])
    val outputType = mapper.treeToValue(rootNode.get("outputType"), classOf[DataSetType[java.lang.Iterable[AnyRef]]])
    new GlobalMaterializedGroupOperator(inputType, outputType)
  }

  private def deserializeGroupByOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val keyDescriptor = mapper.treeToValue(rootNode.get("keyDescriptor"), classOf[TransformationDescriptor[AnyRef, AnyRef]])
    new GroupByOperator(keyDescriptor)
  }
  
  private def deserializeReduceOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val reduceDescriptor = mapper.treeToValue(rootNode.get("reduceDescriptor"), classOf[ReduceDescriptor[AnyRef]])
    val inputType = mapper.treeToValue(rootNode.get("inputType"), classOf[DataSetType[AnyRef]])
    val outputType = mapper.treeToValue(rootNode.get("outputType"), classOf[DataSetType[AnyRef]])
    new ReduceOperator(reduceDescriptor, inputType, outputType)
  }

  private def deserializeSortOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
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

  private def deserializeCartesianOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val inputType0 = mapper.treeToValue(rootNode.get("inputType0"), classOf[DataSetType[AnyRef]])
    val inputType1 = mapper.treeToValue(rootNode.get("inputType1"), classOf[DataSetType[AnyRef]])
    new CartesianOperator(inputType0, inputType1)
  }

  private def deserializeUnionAllOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val inputType0 = mapper.treeToValue(rootNode.get("inputType0"), classOf[DataSetType[AnyRef]])
    new UnionAllOperator(inputType0)
  }

  private def deserializeIntersectOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val inputType0 = mapper.treeToValue(rootNode.get("inputType0"), classOf[DataSetType[AnyRef]])
    new IntersectOperator(inputType0)
  }

  private def deserializeJoinOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val keyDescriptor0 = mapper.treeToValue(rootNode.get("keyDescriptor0"), classOf[TransformationDescriptor[AnyRef, AnyRef]])
    val keyDescriptor1 = mapper.treeToValue(rootNode.get("keyDescriptor1"), classOf[TransformationDescriptor[AnyRef, AnyRef]])
    new JoinOperator(keyDescriptor0, keyDescriptor1)
  }

  private def deserializeCoGroupOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val keyDescriptor0 = mapper.treeToValue(rootNode.get("keyDescriptor0"), classOf[TransformationDescriptor[AnyRef, AnyRef]])
    val keyDescriptor1 = mapper.treeToValue(rootNode.get("keyDescriptor1"), classOf[TransformationDescriptor[AnyRef, AnyRef]])
    new CoGroupOperator(keyDescriptor0, keyDescriptor1)
  }

  private def deserializeDoWhileOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val inputType = mapper.treeToValue(rootNode.get("inputType"), classOf[DataSetType[AnyRef]])
    val convergenceType = mapper.treeToValue(rootNode.get("convergenceType"), classOf[DataSetType[AnyRef]])
    val criterionDescriptor = mapper.treeToValue(rootNode.get("criterionDescriptor"), classOf[PredicateDescriptor[java.util.Collection[AnyRef]]])
    val numExpectedIterations = mapper.treeToValue(rootNode.get("numExpectedIterations"), classOf[Integer])
    new DoWhileOperator(inputType, convergenceType, criterionDescriptor, numExpectedIterations)
  }

  private def deserializeRepeatOperator(jp: JsonParser, rootNode: JsonNode): Operator = {
    val numIterations = mapper.treeToValue(rootNode.get("numIterations"), classOf[Integer])
    val typeValue = mapper.treeToValue(rootNode.get("type"), classOf[DataSetType[AnyRef]])
    new RepeatOperator(numIterations, typeValue)
  }


  private def connectToInputOperatorsAndReturn(node: JsonNode, operator: Operator): Operator = {
    val inputOperators = getInputOperators(node)
    for ((inputOperator, index) <- inputOperators.zipWithIndex) {
      // println(s"Connecting ${inputOperator.getClass.getSimpleName} to ${operator.getClass.getSimpleName}")
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
        if (inputSlot.get("@id") != null) {

          val inputSlotId = inputSlot.get("@id").asLong()
          // println(s"Processing input slot with id ${inputSlotId}")

          val outputSlot = inputSlot.get("occupant")

          // Access owner
          if (outputSlot.get("@id") != null) {

            val outputSlotId = outputSlot.get("@id").asLong
            // println(s"Processing output slot with id ${outputSlotId}")

            val owner = outputSlot.get("owner")

            // Deserialize the nested owner operator and add it into list to be returned
            val jsonParser = owner.traverse(mapper)
            jsonParser.nextToken()
            val inputOperator = mapper.readValue[Operator](jsonParser, classOf[Operator])
            inputOperators = inputOperators :+ inputOperator

            // println(s"\tStoring input slot with id ${inputSlotId}")
            inputSlotOwnerIdMap.get().put(inputSlotId, inputOperator)
            // println(s"\tStoring output slot with id ${outputSlotId}")
            outputSlotOwnerIdMap.get().put(outputSlotId, inputOperator)
          }

          // If owner does not have any fields and is just a number (a Jackson id of an output slot),
          // then it means we have already parsed that node and associated it to an operator
          else {
            val inputOperator = outputSlotOwnerIdMap.get().get(outputSlot.asLong)
            inputOperator match {
              case Some(operator) => inputOperators = inputOperators :+ operator
              case None => throw new WayangException(s"Can't find output slot ${outputSlot.asLong}")
            }
          }
        }

        // If occupant does not have any fields and is just a number a Jackson id of an input slot),
        // then it means we have already parsed that node and associated it to an operator
        else {
          val inputOperator = inputSlotOwnerIdMap.get().get(inputSlot.asLong)
          inputOperator match {
            case Some(operator) => inputOperators = inputOperators :+ operator
            case None => throw new WayangException(s"Can't find input slot ${inputSlot.asLong}")
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


object OperatorDeserializer {

  // operator serialization id -> operator
  private val operatorIdMap: ThreadLocal[mutable.Map[Long, Operator]] = ThreadLocal.withInitial(() => mutable.Map[Long, Operator]())

  // input slot serialization id  -> input slot owner
  private val inputSlotOwnerIdMap: ThreadLocal[mutable.Map[Long, Operator]] = ThreadLocal.withInitial(() => mutable.Map[Long, Operator]())

  // output slot serialization id  -> input slot owner
  private val outputSlotOwnerIdMap: ThreadLocal[mutable.Map[Long, Operator]] = ThreadLocal.withInitial(() => mutable.Map[Long, Operator]())

}