package org.apache.wayang.api.json.operatorfromjson.unary

import com.fasterxml.jackson.annotation.JsonTypeName
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson

@JsonTypeName(OperatorFromJson.OperatorNames.Distinct)
case class DistinctOperatorFromJson(override val id: Long,
                                 override val input: Array[Long],
                                 override val output: Array[Long],
                                 override val cat: String,
                                 override val operatorName: String)
  extends OperatorFromJson(id, input, output, cat, operatorName) {
}

