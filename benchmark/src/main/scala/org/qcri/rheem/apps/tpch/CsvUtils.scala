package org.qcri.rheem.apps.tpch

import org.qcri.rheem.core.api.exception.RheemException

/**
  * Utilities for handling CSV files.
  */
object CsvUtils {

  val dateRegex = """(\d{4})-(\d{2})-(\d{2})""".r

  /**
    * Parse a date string (see [[CsvUtils.dateRegex]]) into a custom [[Int]] representation.
    *
    * @param str to be parsed
    * @return the [[Int]] representation
    */
  def parseDate(str: String): Int = str match {
    case CsvUtils.dateRegex(year, month, day) => year.toInt * 365 + month.toInt * 30 + day.toInt
    case other: String => throw new RheemException(s"Cannot parse '$other' as date.")
  }

}
