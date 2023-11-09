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

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import org.apache.wayang.api.serialization.SerializationUtils
import org.apache.wayang.api.serialization.SerializationUtils.{deserialize, serialize, serializeAsString}
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.function.FunctionDescriptor.SerializablePredicate
import org.apache.wayang.core.function.PredicateDescriptor
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval
import org.apache.wayang.core.optimizer.costs.NestableLoadProfileEstimator
import org.apache.wayang.core.types.BasicDataUnitType
import org.apache.wayang.spark.Spark
import org.junit.{Assert, Test}

import java.io.Serializable

class SerializationUtilsTest {

  //  @Test
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

      val serializedBlossomContext = SerializationUtils.serialize(blossomContext)
      val deserializedBlossomContext = SerializationUtils.deserialize[BlossomContext](serializedBlossomContext)
      Assert.assertEquals(deserializedBlossomContext.getConfiguration.getStringProperty("spark.master"), "random_master_url_1")
      Assert.assertEquals(deserializedBlossomContext.getConfiguration.getStringProperty("spark.app.name"), "random_app_name_2")
      Assert.assertEquals(deserializedBlossomContext.getSink.get.asInstanceOf[BlossomContext.TextFileSink].textFileUrl, "file:///tmp/out11")
      Assert.assertArrayEquals(blossomContext.getConfiguration.getPlatformProvider.provideAll().toArray, deserializedBlossomContext.getConfiguration.getPlatformProvider.provideAll().toArray)
    } catch {
      case t: Throwable =>
        t.printStackTrace()
        throw t
    }
  }

  //  @Test
  def multiContextPlanBuilderSerializationTest(): Unit = {
    val configuration1 = new Configuration()
    configuration1.setProperty("spark.master", "master1")
    val configuration2 = new Configuration()
    configuration2.setProperty("spark.master", "master2")
    val context1 = new BlossomContext(configuration1).withPlugin(Spark.basicPlugin()).withTextFileSink("file:///tmp/out11")
    val context2 = new BlossomContext(configuration2).withPlugin(Spark.basicPlugin()).withObjectFileSink("file:///tmp/out12")

    val multiContextPlanBuilder = MultiContextPlanBuilder(List(context1, context2))
      .withUdfJarsOf(classOf[SerializationUtilsTest])

    try {
      val serialized = SerializationUtils.serializeAsString(multiContextPlanBuilder)
      val deserialized = SerializationUtils.deserializeFromString[MultiContextPlanBuilder](serialized)
      log(SerializationUtils.serializeAsString(deserialized))
      Assert.assertEquals(
        multiContextPlanBuilder.withClassesOf.get.toList,
        deserialized.withClassesOf.get.toList
      )
      Assert.assertEquals(
        multiContextPlanBuilder.contexts(0).getConfiguration.getStringProperty("spark.master"),
        "master1"
      )
      Assert.assertEquals(
        multiContextPlanBuilder.contexts(1).getConfiguration.getStringProperty("spark.master"),
        "master2"
      )
      Assert.assertEquals(
        multiContextPlanBuilder.contexts(0).getSink.get.asInstanceOf[BlossomContext.TextFileSink].textFileUrl,
        "file:///tmp/out11"
      )
      Assert.assertEquals(
        multiContextPlanBuilder.contexts(1).getSink.get.asInstanceOf[BlossomContext.ObjectFileSink].textFileUrl,
        "file:///tmp/out12"
      )
    }
    catch {
      case t: Throwable =>
        t.printStackTrace()
        throw t
    }
  }

  @Test
  def dataQuantaSerializationTest(): Unit = {
    val configuration = new Configuration()
    val wayangContext = new WayangContext(configuration).withPlugin(Spark.basicPlugin())
    val dataQuanta = new PlanBuilder(wayangContext)
      .readTextFile("file:///tmp/in1.txt")
//      .map(s => s + " Wayang out.")
      .filter(s => s.length > 20)

    try {
      val serialized = SerializationUtils.serializeAsString(dataQuanta)
      log(serialized, "gia-na-doume2.json")
      val deserialized = SerializationUtils.deserializeFromString[DataQuanta[AnyRef]](serialized)
    }
    catch {
      case t: Throwable =>
        t.printStackTrace()
        throw t
    }
  }

//  @Test
  def testPredicateDescriptor(): Unit = {

    val predicateDescriptor: PredicateDescriptor[Int] = new PredicateDescriptor[Int](
      null,
      basicDataUnitType[Int],
      ProbabilisticDoubleInterval.ofExactly(1.23),
      new NestableLoadProfileEstimator((f1: Long, f2: Long) => f1 * f2, (in: Long, out: Long) => 0L)
    )

    val serialized = SerializationUtils.serializeAsString(predicateDescriptor)
    log(serialized)
    val deserialized = SerializationUtils.deserializeFromString[PredicateDescriptor[Int]](serialized)
  }

  def log(text: String, filename: String = "customLogFile.json"): Unit = {
    import java.io.{File, FileWriter}

    // Get the user's desktop path
    val desktopPath = System.getProperty("user.home") + "/Desktop"

    // Specify the filename for your custom file
    val customFile = new File(desktopPath, filename)

    // Create the file if it doesn't exist, or overwrite it if it does
    customFile.createNewFile() // This will not harm the existing file

    // Write to the temp file using a FileWriter
    val writer = new FileWriter(customFile)
    try {
      writer.write(text)
    } finally {
      writer.close()
    }

    // If you want to log the path of the temporary file
    println(s"Temp file created at: ${customFile.getAbsolutePath}")
  }

}
