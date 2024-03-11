package org.apache.wayang.api.json.springboot.restapi

import org.apache.wayang.api.json.builder.JsonPlanBuilder
import org.apache.wayang.api.json.exception.WayangApiJsonException
import org.apache.wayang.api.json.operatorfromdrawflow.OperatorFromDrawflowConverter
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson
import org.springframework.http.{HttpStatus, ResponseEntity}
import org.springframework.web.bind.annotation.{ExceptionHandler, PostMapping, RequestBody, RequestMapping, RestController}
import org.apache.wayang.api.json.parserutil.{ParseOperatorsFromDrawflow, ParseOperatorsFromJson}


@RequestMapping(path = Array("/wayang-api-json"))
@RestController
class WayangApiJson {

  @PostMapping(Array("submit-plan/drawflow-format")) def submitPlanDrawflowFormat(@RequestBody requestBody: String): String = {

    println("GOT THE FOLLOWING")
    println(requestBody)
    println()
    println()

    val operatorsFromDrawflow = ParseOperatorsFromDrawflow.parseOperatorsFromString(requestBody).get
    val operatorsFromJson = operatorsFromDrawflow.flatMap(op => OperatorFromDrawflowConverter.toOperatorFromJson(op))

    val result = new JsonPlanBuilder()
      .setOperators(operatorsFromJson)
      .execute()

    if (operatorsFromJson.exists(op => op.cat == OperatorFromJson.Categories.Output))
      "Success"
    else
      result.collect().toString()
  }

  @PostMapping(Array("submit-plan")) def submitPlan(@RequestBody requestBody: String): String = {
    val operators = ParseOperatorsFromJson.parseOperatorsFromString(requestBody).get

    val result = new JsonPlanBuilder()
      .setOperators(operators)
      .execute()

    if (operators.exists(op => op.cat == OperatorFromJson.Categories.Output))
      "Success"
    else
      result.collect().toString()
  }

  @ExceptionHandler(Array(classOf[WayangApiJsonException]))
  def handleException(e: WayangApiJsonException): ResponseEntity[Any] = {
    ResponseEntity
      .status(HttpStatus.INTERNAL_SERVER_ERROR)
      .body(e.getMessage)
  }

  @ExceptionHandler(Array(classOf[Exception]))
  def handleException(e: Exception): ResponseEntity[Any] = {
    e.printStackTrace()
    ResponseEntity
      .status(HttpStatus.INTERNAL_SERVER_ERROR)
      .body("Something went wrong!")
  }

}
