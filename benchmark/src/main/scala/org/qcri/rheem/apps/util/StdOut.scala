package org.qcri.rheem.apps.util

import java.util.Objects

/**
  * Utility for printing to the stdout.
  */
object StdOut {

  def printLimited[T](iterable: Iterable[T],
                      limit: Int = 10,
                      formatter: T => String = (t: T) => Objects.toString(t)): Unit = {
    iterable.take(limit).map(formatter).foreach(println)
    val numRemainders = iterable.size - limit
    if (numRemainders > 0) {
      println(f"...and $numRemainders%,d more.")
    }
  }

}
