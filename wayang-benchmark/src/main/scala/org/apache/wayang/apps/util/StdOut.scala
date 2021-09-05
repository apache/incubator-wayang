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

package org.apache.wayang.apps.util

import java.util.Objects

/**
  * Utility for printing to the stdout.
  */
object StdOut {

  def printLimited[T](iterable: Iterable[T],
                      limit: Int = 10,
                      formatter: T => String = (t: T) => Objects.toString(t)): Unit = {
    iterable.take(limit).map(formatter).foreach(println)
    val numRemainders = iterable.size - limit
    if (numRemainders > 0) {
      println(f"...and $numRemainders%,d more.")
    }
  }

}
