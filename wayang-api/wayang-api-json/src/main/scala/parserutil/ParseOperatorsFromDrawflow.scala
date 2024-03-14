package org.apache.wayang.api.json.parserutil

import org.apache.wayang.api.json.operatorfromdrawflow.OperatorFromDrawflow
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.wayang.api.json.exception.WayangApiJsonException

import scala.io.Source


object ParseOperatorsFromDrawflow {

  def parseOperatorsFromString(string: String): Option[List[OperatorFromDrawflow]] = {
    try {
      val mapper = JsonMapper.builder()
        .addModule(DefaultScalaModule)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .build()
      val operators = mapper.readTree(string).get("pipeline").get("data").toString
      val ret = Some(mapper.readValue(operators, new TypeReference[Map[String, OperatorFromDrawflow]] {}))
      Some(ret.get.values.toList)
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        throw new WayangApiJsonException("Can't parse drawflow json plan.")
    }
  }

  def parseOperatorsFromFile(filename: String): Option[List[OperatorFromDrawflow]] = {
    val resourceStream = getClass.getClassLoader.getResourceAsStream(filename)
    val fileSource = Source.fromInputStream(resourceStream)
    val ret = parseOperatorsFromString(fileSource.mkString)
    fileSource.close()
    ret
  }

}
