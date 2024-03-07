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

/**
  * Represents elements from the TPC-H `SUPPLIER` table.
  */
case class Supplier(supplierKey: Long,
                    name: String,
                    address: String,
                    nationKey: Long,
                    phone: String,
                    acctbal: Double,
                    comment: String)

object Supplier {

  val fields = IndexedSeq("s_supplierkey", "s_name", "s_address","s_nationkey","s_phone","s_acctbal","s_comment")

  /**
    * Parse a CSV row into a [[Supplier]] instance.
    *
    * @param csv the [[String]] to parse
    * @return the [[Supplier]]
    */
  def parseCsv(csv: String): Supplier = {
    val fields = csv.split("\\|")

    Supplier(
      fields(0).toLong,
      fields(1),
      fields(2),
      fields(3).toLong,
      fields(4),
      fields(5).toDouble,
      fields(6),
    )
  }

  def toTuple(s: Supplier): (Long, String, String, Long, String, Double, String) = {
    (s.supplierKey, s.name, s.address, s.nationKey, s.phone, s.acctbal, s.comment)
  }

}
