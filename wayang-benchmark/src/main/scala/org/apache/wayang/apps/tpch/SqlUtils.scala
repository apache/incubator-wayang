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
