package org.qcri.rheem.apps.simwords

import java.lang.Iterable
import java.util

import org.qcri.rheem.core.function.ExecutionContext
import org.qcri.rheem.core.function.FunctionDescriptor.ExtendedSerializableFunction

/**
  * UDF to split text lines and scrub the tokens.
  */
class ScrubFunction extends ExtendedSerializableFunction[String, java.lang.Iterable[String]] {

  lazy val textScrubber = new TextScrubber

  override def open(ctx: ExecutionContext): Unit = {}

  override def apply(line: String): Iterable[String] = {
    val result = new util.LinkedList[String]()
    textScrubber.splitAndScrub(line, result)
    result
  }

}
