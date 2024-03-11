package org.apache.wayang.api.json.operatorfromjson.unary

import com.fasterxml.jackson.annotation.JsonTypeName
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson

@JsonTypeName(OperatorFromJson.OperatorNames.Reduce)
case class ReduceOperatorFromJson(override val id: Long,
                                    override val input: Array[Long],
                                    override val output: Array[Long],
                                    override val cat: String,
                                    override val operatorName: String,
                                    val data: ReduceOperatorFromJson.Data)
  extends OperatorFromJson(id, input, output, cat, operatorName) {
}

object ReduceOperatorFromJson {
  case class Data(udf: String)
}
