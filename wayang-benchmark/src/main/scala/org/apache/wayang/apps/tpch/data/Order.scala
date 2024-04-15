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
    val fields = csv.split('|').map(_.trim)

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

  def toTuple(o: Order): (Long, Long, String, Double, Int, String, String, Int, String) = {
    (o.orderKey, o.custKey, o.orderStatus.toString, o.totalPrice, o.orderDate, o.orderPriority, o.clerk, o.shipPrioritiy, o.comment)
  }

}
