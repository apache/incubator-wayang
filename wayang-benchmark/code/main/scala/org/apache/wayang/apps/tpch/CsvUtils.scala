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

package org.apache.wayang.apps.tpch

import org.apache.wayang.core.api.exception.WayangException

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
    case other: String => throw new WayangException(s"Cannot parse '$other' as date.")
  }

}
