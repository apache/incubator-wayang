package org.apache.wayang.api.json.operatorfromjson.input

import com.fasterxml.jackson.annotation.JsonTypeName
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson

@JsonTypeName(OperatorFromJson.OperatorNames.Table)
case class TableInputFromJson(override val id: Long,
                              override val input: Array[Long],
                              override val output: Array[Long],
                              override val cat: String,
                              override val operatorName: String,
                              val data: TableInputFromJson.Data,
                              override val executionPlatform: String = null)
  extends OperatorFromJson(id, input, output, cat, operatorName, executionPlatform) {
}

object TableInputFromJson {
  case class Data(tableName: String, columnNames: List[String])
}
