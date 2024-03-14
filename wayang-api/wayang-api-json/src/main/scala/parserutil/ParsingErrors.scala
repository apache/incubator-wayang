package org.apache.wayang.api.json.parserutil

import org.apache.wayang.api.json.exception.WayangApiJsonException

object ParsingErrors {
  def handleException(e: Throwable, s: String): Nothing = {
    val exceptionMessage = e.getMessage.replaceAll("\r\n|\r|\n", " ")
    val message = s"${exceptionMessage} at:\n\n" + s
    e.printStackTrace()
    println(message)
    throw new WayangApiJsonException(message)
  }
}
