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

package org.apache.wayang.multicontext

import org.apache.wayang.core.api.Configuration

package object apps {

  def loadConfig(args: Array[String]): (Configuration, Configuration) = {
    if (args.length < 2) {
      println("Loading default configurations.")
      (new Configuration(), new Configuration())
    } else {
      println("Loading custom configurations.")
      (loadConfigFromUrl(args(0)), loadConfigFromUrl(args(1)))
    }
  }

  private def loadConfigFromUrl(url: String): Configuration = {
    try {
      new Configuration(url)
    } catch {
      case unexpected: Exception =>
        unexpected.printStackTrace()
        println(s"Can't load configuration from $url")
        new Configuration()
    }
  }

}
