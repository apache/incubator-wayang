package org.qcri.rheem.apps.tpch

/**
  * Utilities to handle SQL-related applications.
  */
object SqlUtils {

  /**
    * Convert a [[java.sql.Date]] to our internal [[Int]] representation.
    * @param date to be converted
    * @return the [[Int]] representation
    */
  @SuppressWarnings(Array("deprecated"))
  def toInt(date: java.sql.Date) = (date.getYear + 1900) * 10000 + date.getMonth * 100 + date.getDate

}
