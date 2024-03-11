package org.apache.wayang.api.json.operatorfromjson.input

import com.fasterxml.jackson.annotation.JsonTypeName
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson

@JsonTypeName(OperatorFromJson.OperatorNames.TextFileInput)
case class TextFileInputFromJson(override val id: Long,
                                 override val input: Array[Long],
                                 override val output: Array[Long],
                                 override val cat: String,
                                 override val operatorName: String,
                                 val data: TextFileInputFromJson.Data)
  extends OperatorFromJson(id, input, output, cat, operatorName) {
}

object TextFileInputFromJson {
  case class Data(filename: String)
}
