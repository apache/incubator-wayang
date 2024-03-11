package org.apache.wayang.api.json.operatorfromjson.output

import com.fasterxml.jackson.annotation.JsonTypeName
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson

@JsonTypeName(OperatorFromJson.OperatorNames.TextFileOutput)
case class TextFileOutputFromJson(override val id: Long,
                                  override val input: Array[Long],
                                  override val output: Array[Long],
                                  override val cat: String,
                                  override val operatorName: String,
                                  val data: TextFileOutputFromJson.Data)
  extends OperatorFromJson(id, input, output, cat, operatorName) {
}

object TextFileOutputFromJson {
  case class Data(filename: String)
}
