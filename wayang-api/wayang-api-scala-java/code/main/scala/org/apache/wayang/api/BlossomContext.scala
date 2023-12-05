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

package org.apache.wayang.api

import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.plugin.Plugin

class BlossomContext(configuration: Configuration) extends WayangContext(configuration) {

  val id: Long = BlossomContext.nextId()

  private var sink: Option[BlossomContext.UnarySink] = None
  private var plugins: List[String] = List()

  def this() = {
    this(new Configuration())
  }

  override def withPlugin(plugin: Plugin): BlossomContext = {
    this.plugins = this.plugins :+ plugin.getClass.getName
    super.withPlugin(plugin)
    this
  }

  def withTextFileSink(url: String): BlossomContext = {
    this.sink = Some(BlossomContext.TextFileSink(url))
    this
  }

  def withObjectFileSink(url: String): BlossomContext = {
    this.sink = Some(BlossomContext.ObjectFileSink(url))
    this
  }

  def withMergeFileSink(url: String): BlossomContext = {
    this.sink = Some(BlossomContext.ObjectFileSink(url))
    this
  }

  def getSink: Option[BlossomContext.UnarySink] = sink
  def getPlugins: List[String] = plugins
}

object BlossomContext {

  private var lastId = 0L

  private def nextId(): Long = {
    lastId += 1
    lastId
  }

  private[api] trait UnarySink
  private[api] case class TextFileSink(textFileUrl: String) extends UnarySink
  private[api] case class ObjectFileSink(textFileUrl: String) extends UnarySink
}
