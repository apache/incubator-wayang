package org.qcri.rheem.apps.tpch.data

import org.qcri.rheem.apps.tpch.CsvUtils

/**
  * Represents elements from the TPC-H `ORDERS` table.
  */
case class Order(orderKey: Long,
                 custKey: Long,
                 orderStatus: Char,
                 totalPrice: Double,
                 orderDate: Int,
                 orderPriority: String,
                 clerk: String,
                 shipPrioritiy: Int,
                 comment: String)

object Order {

  val fields = IndexedSeq(
    "o_orderkey",
    "o_custkey",
    "o_orderstatus",
    "o_totalprice",
    "o_orderdate",
    "o_orderpriority",
    "o_clerk",
    "o_shippriority",
    "o_comment"
  )

  /**
    * Parse a CSV row into a [[Order]] instance.
    *
    * @param csv the [[String]] to parse
    * @return the [[Order]]
    */
  def parseCsv(csv: String): Order = {
    val fields = csv.split("\\|")

    Order(
      fields(0).toLong,
      fields(1).toLong,
      fields(2).charAt(0),
      fields(3).toDouble,
      CsvUtils.parseDate(fields(4)),
      fields(5),
      fields(6),
      fields(7).toInt,
      fields(8)
    )
  }

}
