package org.apache.wayang.api.json.operatorfromjson.loop

import com.fasterxml.jackson.annotation.JsonTypeName
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson

@JsonTypeName(OperatorFromJson.OperatorNames.Repeat)
case class RepeatOperatorFromJson(override val id: Long,
                                  override val input: Array[Long],
                                  override val output: Array[Long],
                                  override val cat: String,
                                  override val operatorName: String,
                                  val data: RepeatOperatorFromJson.Data,
                                  override val executionPlatform: String = null)
  extends OperatorFromJson(id, input, output, cat, operatorName, executionPlatform) {
}

object RepeatOperatorFromJson {
  case class Data(n: Int, bodyBuilder: String)
}
