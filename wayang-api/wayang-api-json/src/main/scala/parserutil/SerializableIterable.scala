package org.apache.wayang.api.json.parserutil

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

class SerializableIterable(string: String) extends Serializable {
  @transient lazy val iterable: Iterable[Any] = {
    val toolbox = runtimeMirror(getClass.getClassLoader).mkToolBox()
    try {
      val tree = toolbox.parse(string)
      toolbox.compile(tree)().asInstanceOf[Iterable[Any]]
    }
    catch {
      case e: Throwable =>
        ParsingErrors.handleException(e, string)
    }
  }

  def get: Iterable[Any] = iterable
}

object SerializableIterable {
  def create(string: String): SerializableIterable = {
    new SerializableIterable(string)
  }
}

