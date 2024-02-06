/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.apps.tpch.data

import org.apache.wayang.apps.tpch.CsvUtils
import org.apache.wayang.core.api.exception.WayangException

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
      case e: Exception => throw new WayangException(s"Could not parse '$csv' (${fields.mkString(", ")}.", e)
    }
  }

}
