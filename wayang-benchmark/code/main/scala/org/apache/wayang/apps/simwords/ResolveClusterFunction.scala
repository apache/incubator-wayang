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

import org.apache.wayang.core.function.ExecutionContext
import org.apache.wayang.core.function.FunctionDescriptor.ExtendedSerializableFunction

import scala.collection.JavaConversions._

/**
  * This function creates word neighborhood vectors out of a text.
  */
class ResolveClusterFunction(dictionaryBroadcastName: String)
  extends ExtendedSerializableFunction[List[Int], List[String]] {

  private var dictionary: Map[Int, String] = _

  override def open(ctx: ExecutionContext): Unit = {
    this.dictionary = ctx.getBroadcast[(String, Int)](dictionaryBroadcastName).map(_.swap).toMap
  }

  override def apply(ids: List[Int]): List[String] =
    ids.map(id => dictionary.getOrElse(id, "???"))
}
