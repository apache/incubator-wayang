package org.qcri.rheem.apps.tpch.data

import org.qcri.rheem.apps.tpch.CsvUtils
import org.qcri.rheem.core.api.exception.RheemException

/**
  * Represents elements from the TPC-H `LINEITEM` table.
  */
case class LineItem(orderKey: Long,
                    partKey: Long,
                    suppKey: Long,
                    lineNumber: Int,
                    quantity: Double,
                    extendedPrice: Double,
                    discount: Double,
                    tax: Double,
                    returnFlag: Char,
                    lineStatus: Char,
                    shipDate: Int,
                    commitDate: Int,
                    receiptDate: Int,
                    shipInstruct: String,
                    shipMode: String,
                    comment: String)

object LineItem {

  val fields = IndexedSeq("l_orderkey",
    "l_partkey",
    "l_suppkey",
    "l_linenumber",
    "l_quantity",
    "l_extendedprice",
    "l_discount",
    "l_tax",
    "l_returnflag",
    "l_linestatus",
    "l_shipdate",
    "l_commitdate",
    "l_receiptdate",
    "l_shipinstruct",
    "l_shipmode",
    "l_comment")

  /**
    * Parse a CSV row into a [[LineItem]] instance.
    *
    * @param csv the [[String]] to parse
    * @return the [[LineItem]]
    */
  def parseCsv(csv: String): LineItem = {
    val fields = csv.split("\\|")

    try {
      LineItem(
        fields(0).toLong,
        fields(1).toLong,
        fields(2).toLong,
        fields(3).toInt,
        fields(4).toDouble,
        fields(5).toDouble,
        fields(6).toDouble,
        fields(7).toDouble,
        fields(8).charAt(0),
        fields(9).charAt(0),
        CsvUtils.parseDate(fields(10)),
        CsvUtils.parseDate(fields(11)),
        CsvUtils.parseDate(fields(12)),
        fields(13),
        fields(14),
        fields(15)
      )
    } catch {
      case e: Exception => throw new RheemException(s"Could not parse '$csv' (${fields.mkString(", ")}.", e)
    }
  }

}
