package org.apache.wayang.api.json.operatorfromdrawflow

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson
import org.apache.wayang.api.json.operatorfromjson.binary.{CartesianOperatorFromJson, CoGroupOperatorFromJson, IntersectOperatorFromJson, JoinOperatorFromJson, UnionOperatorFromJson}
import org.apache.wayang.api.json.operatorfromjson.input._
import org.apache.wayang.api.json.operatorfromjson.loop.{DoWhileOperatorFromJson, ForeachOperatorFromJson, RepeatOperatorFromJson}
import org.apache.wayang.api.json.operatorfromjson.output._
import org.apache.wayang.api.json.operatorfromjson.unary._
import org.apache.wayang.api.json.parserutil.ParseOperatorsFromDrawflow

import scala.collection.mutable.ListBuffer

object OperatorFromDrawflowConverter {

  def extractInfo(operatorFromDrawflow: OperatorFromDrawflow): (Long, String, String, Array[Long], Array[Long]) = {
    val id = operatorFromDrawflow.id
    val cat = operatorFromDrawflow.data("cat").asInstanceOf[String]
    val operatorName = operatorFromDrawflow.data("operatorName").asInstanceOf[String]
    val input = operatorFromDrawflow.inputs.values
      .map(inputs => inputs.connections)
      .flatMap(connections => connections.map(connection => connection.node.toLong))
      .toArray
    val output = operatorFromDrawflow.outputs.values
      .map(outputs => outputs.connections)
      .flatMap(connections => connections.map(connection => connection.node.toLong))
      .toArray
    (id, cat, operatorName, input, output)
  }

  def toOperatorFromJson(operatorFromDrawflow: OperatorFromDrawflow): List[OperatorFromJson] = {

    val (id, cat, operatorName, composedOperatorInput, composedOperatorOutput) = extractInfo(operatorFromDrawflow)

    if (cat == "composed") {
      val objectMapper = JsonMapper.builder()
        .addModule(DefaultScalaModule)
        .build()

      val operatorsFromDrawflow = ParseOperatorsFromDrawflow.parseOperatorsFromString(objectMapper.writeValueAsString(operatorFromDrawflow.data)).get
      val operatorsFromJson = operatorsFromDrawflow.flatMap(op => toOperatorFromJson(op)).toBuffer

      val indexOfFirstOperatorFromJson = operatorsFromJson.indexOf(operatorsFromJson.filter(op => op.input.length == 0).toList.head)
      val indexOfLastOperatorFromJson = operatorsFromJson.indexOf(operatorsFromJson.filter(op => op.output.length == 0).toList.head)

      operatorsFromJson(indexOfLastOperatorFromJson) = operatorsFromJson(indexOfLastOperatorFromJson).replaceId(id)
      operatorsFromJson(indexOfLastOperatorFromJson) = operatorsFromJson(indexOfLastOperatorFromJson).replaceOutputArray(composedOperatorOutput)
      operatorsFromJson(indexOfFirstOperatorFromJson) = operatorsFromJson(indexOfFirstOperatorFromJson).replaceInputArray(composedOperatorInput)

      operatorsFromJson.toList
    }
    else {
      List(toOperatorFromJsonBody(operatorFromDrawflow))
    }

  }

  private def toOperatorFromJsonBody(operatorFromDrawflow: OperatorFromDrawflow): OperatorFromJson = {

    val (id, cat, operatorName, input, output) = extractInfo(operatorFromDrawflow)

    operatorName match {
      // input
      case "iBinary" => InputCollectionFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.InputCollection, InputCollectionFromJson.Data(operatorFromDrawflow.data("collectionGeneratorFunction").asInstanceOf[String]))
      case "iTextFile" => TextFileInputFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.TextFileInput, TextFileInputFromJson.Data(operatorFromDrawflow.data("inputFileURL").asInstanceOf[String]))
      case "iCsvFile" => TableInputFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Table, TableInputFromJson.Data(operatorFromDrawflow.data("tableName").asInstanceOf[String], operatorFromDrawflow.data("columnNames").asInstanceOf[String].split(",").map(s => s.trim()).toList))
      case "iJdbc" => JDBCRemoteInputFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.JDBCRemoteInput,
        JDBCRemoteInputFromJson.Data(
          operatorFromDrawflow.data("inputJDBCconnection").asInstanceOf[String],
          operatorFromDrawflow.data("usernameJDBCconnection").asInstanceOf[String],
          operatorFromDrawflow.data("passwordJDBCconnection").asInstanceOf[String],
          operatorFromDrawflow.data("tableJDBCconnection").asInstanceOf[String],
          operatorFromDrawflow.data("columnNamesJDBCconnection").asInstanceOf[String].split(",").map(s => s.trim()).toList
        ))

      // unary
      case "filter" => FilterOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Filter, FilterOperatorFromJson.Data(operatorFromDrawflow.data("booleanFunction").asInstanceOf[String]))
      case "reduceBy" => ReduceByOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.ReduceBy, ReduceByOperatorFromJson.Data(operatorFromDrawflow.data("keyFunction").asInstanceOf[String], operatorFromDrawflow.data("reduceFunction").asInstanceOf[String]))
      case "count" => CountOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Count)
      case "groupBy" => GroupByOpeartorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.GroupBy, GroupByOpeartorFromJson.Data(operatorFromDrawflow.data("keyFunction").asInstanceOf[String]))
      case "sort" => SortOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Sort, SortOperatorFromJson.Data(operatorFromDrawflow.data("keyFunction").asInstanceOf[String]))
      case "flatMap" => FlatMapOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.FlatMap, FlatMapOperatorFromJson.Data(operatorFromDrawflow.data("flatMapFunction").asInstanceOf[String]))
      case "map" => MapOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Map, MapOperatorFromJson.Data(operatorFromDrawflow.data("mapFunction").asInstanceOf[String]))
      case "reduce" => ReduceOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Reduce, ReduceOperatorFromJson.Data(operatorFromDrawflow.data("reduceFunction").asInstanceOf[String]))
      case "distinct" => DistinctOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Distinct)
      case "mapPartitions" => MapPartitionsOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.MapPartitions, MapPartitionsOperatorFromJson.Data(operatorFromDrawflow.data("mapPartitionsFunction").asInstanceOf[String]))
      case "sample" => SampleOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Sample, SampleOperatorFromJson.Data(operatorFromDrawflow.data("sampleSize").asInstanceOf[String].toInt))

      // binary
      case "union" => UnionOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Union)
      case "coGroup" => CoGroupOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.CoGroup, CoGroupOperatorFromJson.Data(operatorFromDrawflow.data("groupKey1").asInstanceOf[String], operatorFromDrawflow.data("groupKey2").asInstanceOf[String]))
      case "cartesian" => CartesianOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Cartesian)
      case "join" => JoinOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Join, JoinOperatorFromJson.Data(operatorFromDrawflow.data("joinKey1").asInstanceOf[String], operatorFromDrawflow.data("joinKey2").asInstanceOf[String]))
      case "intersect" => IntersectOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Intersect)

      // loop
      case "foreach" => ForeachOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Foreach, ForeachOperatorFromJson.Data(operatorFromDrawflow.data("Body").asInstanceOf[String]))
      case "while" => DoWhileOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.TextFileOutput, DoWhileOperatorFromJson.Data(operatorFromDrawflow.data("criterionFunction").asInstanceOf[String], operatorFromDrawflow.data("Body").asInstanceOf[String]))
      case "repeat" => RepeatOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Repeat, RepeatOperatorFromJson.Data(operatorFromDrawflow.data("numberOfIterations").asInstanceOf[String].toInt, operatorFromDrawflow.data("Body").asInstanceOf[String]))

      // output
      case "oTextFile" => TextFileOutputFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.TextFileOutput, TextFileOutputFromJson.Data(operatorFromDrawflow.data("outputFileURL").asInstanceOf[String]))
    }

  }

}
