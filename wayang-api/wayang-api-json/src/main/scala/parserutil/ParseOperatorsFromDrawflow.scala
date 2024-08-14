/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
