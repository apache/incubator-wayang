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


package org.apache.wayang.apps.multicontext

import org.apache.wayang.api.{BlossomContext, MultiContextPlanBuilder}
import org.apache.wayang.core.api.Configuration
import org.apache.wayang.java.Java

class WordCount {}

object WordCount {

  def main(args: Array[String]): Unit = {
    val context1 = new BlossomContext(new Configuration()).withPlugin(Java.basicPlugin()).withTextFileSink("file:///tmp/out11")
    val context2 = new BlossomContext(new Configuration()).withPlugin(Java.basicPlugin()).withTextFileSink("file:///tmp/out12")
    val multiContextPlanBuilder = MultiContextPlanBuilder(List(context1, context2))
      .withUdfJarsOf(classOf[WordCount])

    // Generate some test data
    val inputValues = Array("Big data is big.", "Is data big data?")

    // Build and execute a word count
    multiContextPlanBuilder
      .loadCollection(inputValues)
      .flatMap(_.split("\\s+"))
      .map(_.replaceAll("\\W+", "").toLowerCase)
      .map((_, 1))
      .reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2))
      .execute()
  }

}

