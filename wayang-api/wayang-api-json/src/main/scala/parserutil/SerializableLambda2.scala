package org.apache.wayang.api.json.parserutil

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

class SerializableLambda2[A, B, C](string: String) extends ((A, B) => C) with Serializable {
  @transient lazy val lambdaFunction: (A, B) => C = {
    val toolbox = runtimeMirror(getClass.getClassLoader).mkToolBox()
    try {
      val tree = toolbox.parse(string)
      toolbox.compile(tree)().asInstanceOf[(A, B) => C]
    }
    catch {
      case e: Throwable =>
        ParsingErrors.handleException(e, string)
    }
  }

  def apply(arg1: A, arg2: B): C = lambdaFunction(arg1, arg2)
}

object SerializableLambda2 {
  def createLambda[A, B, C](string: String): SerializableLambda2[A, B, C] = {
    new SerializableLambda2[A, B, C](string)
  }
}

