package org.apache.wayang.api.json.parserutil

import org.apache.wayang.api.json.exception.WayangApiJsonException
import org.apache.wayang.api.json.parserutil.MyTypeName

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

object ParseScalaFromString {

  private def handleException(e: Throwable): Nothing = {
    e.printStackTrace()
    throw new WayangApiJsonException("You have an error in your scala lambda syntax!")
  }

  def parseLambda2ImportDataQuanta[IN, OUT](string: String): IN => OUT = {
    try {
      val imports = "import org.apache.wayang.api.DataQuanta"
      val code = imports + "\n" + string
      parseLambda2[IN, OUT](code)
    }
    catch {
      case e: Throwable =>
        handleException(e)
    }
  }

  def parseLambda3[Input: MyTypeName](string: String): Any => Any = {
    val code =
        s"""val result: ${implicitly[MyTypeName[Input]].name} => Any = ${string}
         |result
         |""".stripMargin
    println("parseLambda3")
    println(code)
    val toolbox = runtimeMirror(getClass.getClassLoader).mkToolBox()
    val tree = toolbox.parse(code)
    val lambdaFunction = toolbox.compile(tree)().asInstanceOf[Any => Any]
    lambdaFunction
  }

  def applyParsedLambda3[In: TypeTag, Out](iterable: Iterable[In], lambdaString: String): Iterable[Out] = {
    println("applyParsedLambda3")
    println("In")
    println(typeOf[In])
    val it = iterable.map(
      parseLambda3[In](lambdaString)
    )
    it.asInstanceOf
  }

  def parseLambda2[IN, OUT](string: String): IN => OUT = {
    try {
      val toolbox = runtimeMirror(getClass.getClassLoader).mkToolBox()
      val tree = toolbox.parse(string)
      val lambdaFunction = toolbox.compile(tree)().asInstanceOf[IN => OUT]
      lambdaFunction
    }
    catch {
      case e: Throwable =>
        handleException(e)
    }
  }

  def parseLambda(string: String): Any => Any = {
    try {
      val toolbox = runtimeMirror(getClass.getClassLoader).mkToolBox()
      val tree = toolbox.parse(string)
      val lambdaFunction = toolbox.compile(tree)().asInstanceOf[Any => Any]
      lambdaFunction
    }
    catch {
      case e: Throwable =>
        handleException(e)
    }
  }

  def parseLambdaTuple2ToAny(string: String): (Any, Any) => Any = {
    try {
      val toolbox = runtimeMirror(getClass.getClassLoader).mkToolBox()
      val tree = toolbox.parse(string)
      val lambdaFunction = toolbox.compile(tree)().asInstanceOf[(Any, Any) => Any]
      lambdaFunction
    }
    catch {
      case e: Throwable =>
        handleException(e)
    }
  }

  def parseIterable(string: String): Iterable[Any] = {
    try {
      val toolbox = runtimeMirror(getClass.getClassLoader).mkToolBox()
      val tree = toolbox.parse(string)
      val lambdaFunction = toolbox.compile(tree)().asInstanceOf[Iterable[Any]]
      lambdaFunction
    }
    catch {
      case e: Throwable =>
        handleException(e)
    }
  }

}
