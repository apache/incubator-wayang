package org.apache.wayang.api.json

import zio._
import zio.IO
import zio.http._
import zio.Console._
import scala.util.Try

import org.apache.wayang.api.json.builder.JsonPlanBuilder
import org.apache.wayang.api.json.parserutil.ParseOperatorsFromJson
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson

object Hello extends ZIOAppDefault {
  val drawRoute =
    Method.GET / "text" -> handler(Response.text("Hello World!"))

  val jsonRoute =
    Method.POST / "json" -> handler { (req: Request) =>
     (for {
        requestBody <- req.body.asString
        operatorsFromJson <- ZIO.fromTry(Try(ParseOperatorsFromJson.parseOperatorsFromString(requestBody).get))
        result <- ZIO.attempt(new JsonPlanBuilder().setOperators(operatorsFromJson).execute())
        responseBody <- ZIO.attempt {
          if (operatorsFromJson.exists(op => op.cat == OperatorFromJson.Categories.Output))
            "Success"
          else
            result.collect().toString()
        }
        resBody <- ZIO.succeed(Response.text(responseBody))
     } yield resBody).orDie
    }

  // Create HTTP route
  val app = Routes(drawRoute, jsonRoute).toHttpApp

  // Run it like any simple app
  override val run = Server.serve(app).provide(Server.default)
}
