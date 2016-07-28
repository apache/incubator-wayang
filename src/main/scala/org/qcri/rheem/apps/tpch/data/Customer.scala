package org.qcri.rheem.apps.tpch.data

/**
  * Represents elements from the TPC-H `CUSTOMER` table.
  */
case class Customer(custKey: Long,
                    name: String,
                    address: String,
                    nationKey: Long,
                    phone: String,
                    acctbal: Double,
                    mktSegment: String,
                    comment: String)

object Customer {

  /**
    * Parse a CSV row into a [[Customer]] instance.
    *
    * @param csv the [[String]] to parse
    * @return the [[Customer]]
    */
  def parseCsv(csv: String): Customer = {
    val fields = csv.substring(1, csv.length - 1).split("\";\"")

    Customer(
      fields(0).toLong,
      fields(1),
      fields(2),
      fields(3).toLong,
      fields(4),
      fields(5).toDouble,
      fields(6).trim,
      fields(7)
    )
  }

}
