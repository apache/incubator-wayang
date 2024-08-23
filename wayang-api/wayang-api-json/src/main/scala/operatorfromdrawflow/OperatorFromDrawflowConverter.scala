/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

object OperatorFromDrawflowConverter {

  private def extractInfo(operatorFromDrawflow: OperatorFromDrawflow): (Long, String, String, Array[Long], Array[Long], String) = {
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

    val executionPlatform = operatorFromDrawflow.data.getOrElse("executionPlatform", null).asInstanceOf[String]
    (id, cat, operatorName, input, output, executionPlatform)
  }

  def toOperatorFromJson(operatorFromDrawflow: OperatorFromDrawflow): List[OperatorFromJson] = {

    val (id, cat, operatorName, composedOperatorInput, composedOperatorOutput, executionPlatform) = extractInfo(operatorFromDrawflow)

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

    val (id, cat, operatorName, input, output, executionPlatform) = extractInfo(operatorFromDrawflow)

    operatorName match {
      // input
      case "iBinary" => InputCollectionFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.InputCollection, InputCollectionFromJson.Data(operatorFromDrawflow.data("collectionGeneratorFunction").asInstanceOf[String]), executionPlatform)
      case "iTextFile" => TextFileInputFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.TextFileInput, TextFileInputFromJson.Data(operatorFromDrawflow.data("inputFileURL").asInstanceOf[String]), executionPlatform)
      case "iCsvFile" => TableInputFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Table, TableInputFromJson.Data(operatorFromDrawflow.data("tableName").asInstanceOf[String], operatorFromDrawflow.data("columnNames").asInstanceOf[String].split(",").map(s => s.trim()).toList), executionPlatform)
      case "iJdbc" => JDBCRemoteInputFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.JDBCRemoteInput,
        JDBCRemoteInputFromJson.Data(
          operatorFromDrawflow.data("inputJDBCconnection").asInstanceOf[String],
          operatorFromDrawflow.data("usernameJDBCconnection").asInstanceOf[String],
          operatorFromDrawflow.data("passwordJDBCconnection").asInstanceOf[String],
          operatorFromDrawflow.data("tableJDBCconnection").asInstanceOf[String],
          operatorFromDrawflow.data("columnNamesJDBCconnection").asInstanceOf[String].split(",").map(s => s.trim()).toList
        ))

      // unary
      case "filter" => FilterOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Filter, FilterOperatorFromJson.Data(operatorFromDrawflow.data("booleanFunction").asInstanceOf[String]), executionPlatform)
      case "reduceBy" => ReduceByOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.ReduceBy, ReduceByOperatorFromJson.Data(operatorFromDrawflow.data("keyFunction").asInstanceOf[String], operatorFromDrawflow.data("reduceFunction").asInstanceOf[String]), executionPlatform)
      case "count" => CountOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Count, executionPlatform)
      case "groupBy" => GroupByOpeartorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.GroupBy, GroupByOpeartorFromJson.Data(operatorFromDrawflow.data("keyFunction").asInstanceOf[String]), executionPlatform)
      case "sort" => SortOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Sort, SortOperatorFromJson.Data(operatorFromDrawflow.data("keyFunction").asInstanceOf[String]), executionPlatform)
      case "flatMap" => FlatMapOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.FlatMap, FlatMapOperatorFromJson.Data(operatorFromDrawflow.data("flatMapFunction").asInstanceOf[String], None, None), executionPlatform)
      case "map" => MapOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Map, MapOperatorFromJson.Data(operatorFromDrawflow.data("mapFunction").asInstanceOf[String], None, None), executionPlatform)
      case "reduce" => ReduceOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Reduce, ReduceOperatorFromJson.Data(operatorFromDrawflow.data("reduceFunction").asInstanceOf[String], None, None), executionPlatform)
      case "distinct" => DistinctOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Distinct, executionPlatform)
      case "mapPartitions" => MapPartitionsOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.MapPartitions, MapPartitionsOperatorFromJson.Data(operatorFromDrawflow.data("mapPartitionsFunction").asInstanceOf[String], None, None), executionPlatform)
      case "sample" => SampleOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Sample, SampleOperatorFromJson.Data(operatorFromDrawflow.data("sampleSize").asInstanceOf[String].toInt), executionPlatform)

      // binary
      case "union" => UnionOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Union, executionPlatform)
      case "coGroup" => CoGroupOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.CoGroup, CoGroupOperatorFromJson.Data(operatorFromDrawflow.data("groupKey1").asInstanceOf[String], operatorFromDrawflow.data("groupKey2").asInstanceOf[String]), executionPlatform)
      case "cartesian" => CartesianOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Cartesian, executionPlatform)
      case "join" => JoinOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Join, JoinOperatorFromJson.Data(operatorFromDrawflow.data("joinKey1").asInstanceOf[String], operatorFromDrawflow.data("joinKey2").asInstanceOf[String]), executionPlatform)
      case "intersect" => IntersectOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Intersect, executionPlatform)

      // loop
      case "foreach" => ForeachOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Foreach, ForeachOperatorFromJson.Data(operatorFromDrawflow.data("Body").asInstanceOf[String]), executionPlatform)
      case "while" => DoWhileOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.TextFileOutput, DoWhileOperatorFromJson.Data(operatorFromDrawflow.data("criterionFunction").asInstanceOf[String], operatorFromDrawflow.data("Body").asInstanceOf[String]), executionPlatform)
      case "repeat" => RepeatOperatorFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.Repeat, RepeatOperatorFromJson.Data(operatorFromDrawflow.data("numberOfIterations").asInstanceOf[String].toInt, operatorFromDrawflow.data("Body").asInstanceOf[String]), executionPlatform)

      // output
      case "oTextFile" => TextFileOutputFromJson(id, input, output, cat, OperatorFromJson.OperatorNames.TextFileOutput, TextFileOutputFromJson.Data(operatorFromDrawflow.data("outputFileURL").asInstanceOf[String]), executionPlatform)
    }

  }

}
