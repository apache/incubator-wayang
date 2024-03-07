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
  * Represents elements from the TPC-H `PART` table.
  */
case class Part(partKey: Long,
                    name: String,
                    mfgr: String,
                    brand: String,
                    partType: String,
                    size: Long,
                    container: String,
                    retailPrice: Double,
                    comment: String)

object Part {

  val fields = IndexedSeq("p_partkey", "p_name","p_mfgr","p_brand","p_type","p_size","p_container","p_retailprice","p_comment")

  /**
    * Parse a CSV row into a [[Part]] instance.
    *
    * @param csv the [[String]] to parse
    * @return the [[Part]]
    */
  def parseCsv(csv: String): Part = {
    val fields = csv.split("\\|")

    Part(
      fields(0).toLong,
      fields(1),
      fields(2),
      fields(3),
      fields(4),
      fields(5).toLong,
      fields(6),
      fields(7).toDouble,
      fields(8)
      
    )
  }

  def toTuple(p: Part): (Long, String, String, String, String, Long, String, Double, String) = {
    (p.partKey, p.name, p.mfgr, p.brand, p.partType, p.size, p.container, p.retailPrice, p.comment)
  }

}
