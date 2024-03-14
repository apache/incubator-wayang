package org.apache.wayang.api.json.parserutil

import org.apache.wayang.api.json.exception.WayangApiJsonException

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

class SerializableLambda[IN, OUT](string: String) extends (IN => OUT) with Serializable {
  @transient lazy val lambdaFunction: IN => OUT = {
    val toolbox = runtimeMirror(getClass.getClassLoader).mkToolBox()
    try {
      val tree = toolbox.parse(string)
      toolbox.compile(tree)().asInstanceOf[IN => OUT]
    }
    catch {
      case e: Throwable =>
        ParsingErrors.handleException(e, string)
    }
  }

  def apply(input: IN): OUT = lambdaFunction(input)
}

object SerializableLambda {
  def createLambda[IN, OUT](string: String): SerializableLambda[IN, OUT] = {
    new SerializableLambda[IN, OUT](string)
  }
}
