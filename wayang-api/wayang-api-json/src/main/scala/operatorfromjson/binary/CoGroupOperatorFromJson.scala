package org.apache.wayang.api.json.operatorfromjson.binary

import com.fasterxml.jackson.annotation.JsonTypeName
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson

@JsonTypeName(OperatorFromJson.OperatorNames.CoGroup)
case class CoGroupOperatorFromJson(override val id: Long,
                                   override val input: Array[Long],
                                   override val output: Array[Long],
                                   override val cat: String,
                                   override val operatorName: String,
                                   val data: CoGroupOperatorFromJson.Data,
                                   override val executionPlatform: String = null)
  extends OperatorFromJson(id, input, output, cat, operatorName, executionPlatform) {
}

object CoGroupOperatorFromJson {
  case class Data(thisKeyUdf: String, thatKeyUdf: String)
}
