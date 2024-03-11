package org.apache.wayang.api.json.operatorfromjson.input

import com.fasterxml.jackson.annotation.JsonTypeName
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson

@JsonTypeName(OperatorFromJson.OperatorNames.InputCollection)
case class InputCollectionFromJson(override val id: Long,
                                         override val input: Array[Long],
                                         override val output: Array[Long],
                                         override val cat: String,
                                         override val operatorName: String,
                                         val data: InputCollectionFromJson.Data)
  extends OperatorFromJson(id, input, output, cat, operatorName) {
}

object InputCollectionFromJson {
  case class Data(udf: String)
}
