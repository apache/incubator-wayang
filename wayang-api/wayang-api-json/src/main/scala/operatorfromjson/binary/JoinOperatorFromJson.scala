package org.apache.wayang.api.json.operatorfromjson.binary

import com.fasterxml.jackson.annotation.JsonTypeName
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson

@JsonTypeName(OperatorFromJson.OperatorNames.Join)
case class JoinOperatorFromJson(override val id: Long,
                                override val input: Array[Long],
                                override val output: Array[Long],
                                override val cat: String,
                                override val operatorName: String,
                                val data: JoinOperatorFromJson.Data)
extends OperatorFromJson(id, input, output, cat, operatorName) {
}

object JoinOperatorFromJson {
  case class Data(thisKeyUdf: String, thatKeyUdf: String)
}
