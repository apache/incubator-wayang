package org.apache.wayang.api.json.operatorfromjson.loop

import com.fasterxml.jackson.annotation.JsonTypeName
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson

@JsonTypeName(OperatorFromJson.OperatorNames.DoWhile)
case class DoWhileOperatorFromJson(override val id: Long,
                                   override val input: Array[Long],
                                   override val output: Array[Long],
                                   override val cat: String,
                                   override val operatorName: String,
                                   val data: DoWhileOperatorFromJson.Data)
  extends OperatorFromJson(id, input, output, cat, operatorName) {
}

object DoWhileOperatorFromJson {
  case class Data(udf: String, bodyBuilder: String)
}
