package org.apache.wayang.api.json.operatorfromjson

import com.fasterxml.jackson.annotation.JsonTypeName

@JsonTypeName(OperatorFromJson.OperatorNames.Composed)
case class ComposedOperatorFromJson(override val id: Long,
                                    override val input: Array[Long],
                                    override val output: Array[Long],
                                    override val cat: String,
                                    override val operatorName: String,
                                    val operators: Array[OperatorFromJson])
  extends OperatorFromJson(id, input, output, cat, operatorName) {
}

