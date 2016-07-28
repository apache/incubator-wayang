package org.qcri.rheem.apps.util

/**
  * Utility for printing to the stdout.
  */
object StdOut {

  def printLimited(iterable: Iterable[_], limit: Int = 10): Unit = {
    iterable.take(limit).foreach(println)
    val numRemainders = iterable.size - limit
    if (numRemainders > 0) {
      println(f"...and $numRemainders%,d more.")
    }
  }

}
