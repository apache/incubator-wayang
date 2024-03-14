package org.apache.wayang.api.json.operatorfromjson.binary

import com.fasterxml.jackson.annotation.JsonTypeName
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson

@JsonTypeName(OperatorFromJson.OperatorNames.Intersect)
case class IntersectOperatorFromJson(override val id: Long,
                                     override val input: Array[Long],
                                     override val output: Array[Long],
                                     override val cat: String,
                                     override val operatorName: String,
                                     override val executionPlatform: String = null)
  extends OperatorFromJson(id, input, output, cat, operatorName, executionPlatform) {
}

