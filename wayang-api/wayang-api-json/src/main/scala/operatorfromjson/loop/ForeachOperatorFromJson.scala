package org.apache.wayang.api.json.operatorfromjson.loop

import com.fasterxml.jackson.annotation.JsonTypeName
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson

@JsonTypeName(OperatorFromJson.OperatorNames.Foreach)
case class ForeachOperatorFromJson(override val id: Long,
                                   override val input: Array[Long],
                                   override val output: Array[Long],
                                   override val cat: String,
                                   override val operatorName: String,
                                   val data: ForeachOperatorFromJson.Data)
  extends OperatorFromJson(id, input, output, cat, operatorName) {
}

object ForeachOperatorFromJson {
  case class Data(udf: String)
}
