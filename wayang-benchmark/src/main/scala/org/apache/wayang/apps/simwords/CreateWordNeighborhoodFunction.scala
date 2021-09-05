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

package org.apache.wayang.apps.simwords

import java.util

import org.apache.wayang.core.function.ExecutionContext
import org.apache.wayang.core.function.FunctionDescriptor.ExtendedSerializableFunction

import scala.collection.JavaConversions._

/**
  * This function creates word neighborhood vectors out of a text.
  */
class CreateWordNeighborhoodFunction(neighborhoodReach: Int, dictionaryBroadcastName: String)
  extends ExtendedSerializableFunction[String, java.lang.Iterable[(Int, SparseVector)]] {

  private var dictionary: Map[String, Int] = _

  private lazy val textScrubber = new TextScrubber

  private lazy val collector = new java.util.ArrayList[String]

  override def open(ctx: ExecutionContext): Unit = {
    this.dictionary = ctx.getBroadcast[(String, Int)](dictionaryBroadcastName).toMap
  }

  override def apply(value: String): java.lang.Iterable[(Int, SparseVector)] = {
    val result = new util.LinkedList[(Int, SparseVector)]()

    this.textScrubber.splitAndScrub(value, this.collector)
    // Make sure that there is at least one neighbor; otherwise, the resulting vector will not support cosine similarity
    if (this.collector.size > 1) {
      val wordIds = this.collector.map(this.dictionary.getOrElse(_, -1))
      for (i <- wordIds.indices) {
        val builder = new SparseVector.Builder
        for (j <- math.max(0, i - neighborhoodReach) until i; if wordIds(j) != -1) {
          builder.add(wordIds(j), 1)
        }
        for (j <- i + 1 until math.min(wordIds.size, i + neighborhoodReach + 1); if wordIds(j) != -1) {
          builder.add(wordIds(j), 1)
        }
        if (!builder.isEmpty) result.add((wordIds(i), builder.build))
      }
      this.collector.clear()
    }

    result
  }
}
