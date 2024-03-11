package org.apache.wayang.api.json.operatorfromjson.unary

import com.fasterxml.jackson.annotation.JsonTypeName
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson


@JsonTypeName(OperatorFromJson.OperatorNames.GroupBy)
case class GroupByOpeartorFromJson(override val id: Long,
                               override val input: Array[Long],
                               override val output: Array[Long],
                               override val cat: String,
                               override val operatorName: String,
                               val data: GroupByOpeartorFromJson.Data)
  extends OperatorFromJson(id, input, output, cat, operatorName) {
}

object GroupByOpeartorFromJson {
  case class Data(keyUdf: String)
}


