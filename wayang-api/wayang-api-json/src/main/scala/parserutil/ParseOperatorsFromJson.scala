package org.apache.wayang.api.json.parserutil

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.wayang.api.json.exception.WayangApiJsonException
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson

import scala.io.Source

object ParseOperatorsFromJson {

  def parseOperatorsFromString(string: String): Option[List[OperatorFromJson]] = {
    try {
      val mapper = JsonMapper.builder()
        .addModule(DefaultScalaModule)
        // .enable(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES)
        .build()
      val ret = Some(mapper.readValue(string, new TypeReference[List[OperatorFromJson]] {}))
      ret
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        throw new WayangApiJsonException("Can't parse json plan.")
    }
  }

  def parseOperatorsFromFile(filename: String): Option[List[OperatorFromJson]] = {
    val resourceStream = getClass.getClassLoader.getResourceAsStream(filename)
    val fileSource = Source.fromInputStream(resourceStream)
    val ret = parseOperatorsFromString(fileSource.mkString)
    fileSource.close()
    ret
  }
}
