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

import org.apache.wayang.core.api.Configuration
import org.apache.wayang.spark.Spark
import org.junit.{Assert, Test}

class SerializationUtilsTest {

  @Test
  def blossomContextSerializationTest(): Unit = {
    val configuration = new Configuration()
    configuration.setProperty("spark.master", "random_master_url_1")
    configuration.setProperty("spark.app.name", "random_app_name_2")
    val blossomContext = new BlossomContext(configuration).withPlugin(Spark.basicPlugin()).withTextFileSink("file:///tmp/out11")

    try {
      val serializedConfiguration = SerializationUtils.serialize(configuration)
      val deserializedConfiguration = SerializationUtils.deserialize[Configuration](serializedConfiguration)
      Assert.assertEquals(deserializedConfiguration.getStringProperty("spark.master"), "random_master_url_1")
      Assert.assertEquals(deserializedConfiguration.getStringProperty("spark.app.name"), "random_app_name_2")

      //      val serializedBlossomContext = SerializationUtils.mapper.writeValueAsString(blossomContext)
      val serializedBlossomContext = SerializationUtils.serialize(blossomContext)
      val deserializedBlossomContext = SerializationUtils.deserialize[BlossomContext](serializedBlossomContext)
      Assert.assertEquals(deserializedBlossomContext.getConfiguration.getStringProperty("spark.master"), "random_master_url_1")
      Assert.assertEquals(deserializedBlossomContext.getConfiguration.getStringProperty("spark.app.name"), "random_app_name_2")
      Assert.assertEquals(deserializedBlossomContext.getSink.get.asInstanceOf[BlossomContext.TextFileSink].textFileUrl, "file:///tmp/out11")
      Assert.assertArrayEquals(blossomContext.getConfiguration.getPlatformProvider.provideAll().toArray, deserializedBlossomContext.getConfiguration.getPlatformProvider.provideAll().toArray)
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        throw e
    }
  }

}
