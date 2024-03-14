package org.apache.wayang.api.json.operatorfromjson.other

import com.fasterxml.jackson.annotation.JsonTypeName
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson


@JsonTypeName(OperatorFromJson.OperatorNames.KMeans)
case class KMeansFromJson(override val id: Long,
                          override val input: Array[Long],
                          override val output: Array[Long],
                          override val cat: String,
                          override val operatorName: String,
                          val data: KMeansFromJson.Data,
                          override val executionPlatform: String = null)
  extends OperatorFromJson(id, input, output, cat, operatorName, executionPlatform) {
}

object KMeansFromJson {
  case class Data(k: Int,
                  maxIterations: Int,
                  distanceUdf: String,
                  sumUdf: String,
                  divideUdf: String,
                  initialCentroidsUdf: String)
}

