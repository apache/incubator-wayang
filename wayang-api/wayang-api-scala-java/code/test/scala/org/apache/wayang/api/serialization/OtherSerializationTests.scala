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


package org.apache.wayang.api.serialization

import org.apache.wayang.api.{BlossomContext, MultiContextDataQuanta, MultiContextPlanBuilder, PlanBuilder}
import org.apache.wayang.basic.operators.TextFileSink
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.plan.wayangplan.{Operator, WayangPlan}
import org.apache.wayang.core.platform.Platform
import org.apache.wayang.core.util.ReflectionUtils
import org.apache.wayang.java.Java
import org.apache.wayang.spark.Spark
import org.junit.{Assert, Test}


class OtherSerializationTests extends SerializationTestBase {

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


  @Test
  def multiContextPlanBuilderSerializationTest(): Unit = {
    val configuration1 = new Configuration()
    configuration1.setProperty("spark.master", "master1")
    val configuration2 = new Configuration()
    configuration2.setProperty("spark.master", "master2")

    val context1 = new BlossomContext(configuration1).withPlugin(Spark.basicPlugin()).withTextFileSink("file:///tmp/out11")
    val context2 = new BlossomContext(configuration2).withPlugin(Spark.basicPlugin()).withObjectFileSink("file:///tmp/out12")

    val multiContextPlanBuilder = new MultiContextPlanBuilder(List(context1, context2))
      .withUdfJarsOf(classOf[OtherSerializationTests])

    try {
      val serialized = SerializationUtils.serializeAsString(multiContextPlanBuilder)
      val deserialized = SerializationUtils.deserializeFromString[MultiContextPlanBuilder](serialized)
      // SerializationTestBase.log(SerializationUtils.serializeAsString(deserialized), testName.getMethodName + ".log.json")

      Assert.assertEquals(
        multiContextPlanBuilder.withClassesOf,
        deserialized.withClassesOf
      )
      Assert.assertEquals(
        multiContextPlanBuilder.blossomContexts(0).getConfiguration.getStringProperty("spark.master"),
        "master1"
      )
      Assert.assertEquals(
        multiContextPlanBuilder.blossomContexts(1).getConfiguration.getStringProperty("spark.master"),
        "master2"
      )
      Assert.assertEquals(
        multiContextPlanBuilder.blossomContexts(0).getSink.get.asInstanceOf[BlossomContext.TextFileSink].textFileUrl,
        "file:///tmp/out11"
      )
      Assert.assertEquals(
        multiContextPlanBuilder.blossomContexts(1).getSink.get.asInstanceOf[BlossomContext.ObjectFileSink].textFileUrl,
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
  def serializeToTempFileTest(): Unit = {
    // Define configuration
    val configuration = new Configuration()
    val wayangContext = new WayangContext(configuration)
      .withPlugin(Java.basicPlugin())
    val planBuilder = new PlanBuilder(wayangContext)
      .withUdfJarsOf(classOf[OtherSerializationTests])

    // Define plan
    val dataQuanta = planBuilder
      .loadCollection(List("12345", "12345678", "1234567890", "1234567890123"))
      .map(s => s + " Wayang out")
      .map(s => (s, "AAAA", "BBBB"))
      .map(s => List(s._1, "a", "b", "c"))
      .filter(s => s.head.length > 20)
      .map(s => s.head)

    val tempfile = MultiContextDataQuanta.writeToTempFileAsString(dataQuanta.operator)
    val operator = MultiContextDataQuanta.readFromTempFileFromString[Operator](tempfile)

    // Attach an output sink to deserialized plan
    val tempFileOut = s"/tmp/${testName.getMethodName}.out"
    val sink = new TextFileSink[AnyRef](s"file://$tempFileOut", classOf[AnyRef])
    operator.connectTo(0, sink, 0)

    // Execute plan
    val plan = new WayangPlan(sink)
    wayangContext.execute(plan, ReflectionUtils.getDeclaringJar(classOf[OtherSerializationTests]))

    // Check results
    val expectedLines = List("1234567890 Wayang out", "1234567890123 Wayang out")
    SerializationTestBase.assertOutputFile(tempFileOut, expectedLines)
  }


  //  @Test
  def multiDataQuantaExecuteTest(): Unit = {

    try {
      // Create blossom contexts
      val out1 = "/tmp/out11"
      val out2 = "/tmp/out12"
      val context1 = new BlossomContext(new Configuration()).withPlugin(Java.basicPlugin()).withTextFileSink(s"file://$out1")
      val context2 = new BlossomContext(new Configuration()).withPlugin(Java.basicPlugin()).withTextFileSink(s"file://$out2")

      // Create multiContextPlanBuilder
      val multiContextPlanBuilder = new MultiContextPlanBuilder(List(context1, context2))
        .withUdfJarsOf(classOf[OtherSerializationTests])

      // Build and execute plan
      multiContextPlanBuilder
        .loadCollection(List("aaabbb", "aaabbbccc", "aaabbbcccddd", "aaabbbcccdddeee"))
        .map(s => s + " Wayang out.")
        .filter(s => s.length > 20)
        .execute()

      // Check results
      val expectedLines = List("aaabbbcccddd Wayang out", "aaabbbcccdddeee Wayang out")
      SerializationTestBase.assertOutputFile(out1, expectedLines)
      SerializationTestBase.assertOutputFile(out2, expectedLines)
    }
    catch {
      case t: Throwable =>
        t.printStackTrace()
        throw t
    }
  }


  @Test
  def platformSerializationTest(): Unit = {
    try {
      val serialized = SerializationUtils.serialize(Java.platform())
      val deserialized = SerializationUtils.deserialize[Platform](serialized)
      Assert.assertEquals(deserialized.getClass.getName, Java.platform().getClass.getName)
    } catch {
      case t: Throwable =>
        t.printStackTrace()
        throw t
    }
  }


  @Test
  def targetPlatformsTest(): Unit = {
    val configuration = new Configuration()
    val wayangContext = new WayangContext(configuration)
      .withPlugin(Java.basicPlugin())
    val planBuilder = new PlanBuilder(wayangContext)
      .withUdfJarsOf(classOf[OtherSerializationTests])

    val dataQuanta = planBuilder
      .loadCollection(List("12345", "12345678", "1234567890", "1234567890123"))
      .map(s => s + " Wayang out").withTargetPlatforms(Spark.platform()).withTargetPlatforms(Java.platform())

    try {
      val serialized = SerializationUtils.serializeAsString(dataQuanta.operator)
      val deserialized = SerializationUtils.deserializeFromString[Operator](serialized)
      Assert.assertEquals(deserialized.getTargetPlatforms.size(), 2)
      val deserializedPlatformNames = deserialized.getTargetPlatforms.toArray.map(p => p.getClass.getName)
      Assert.assertTrue(deserializedPlatformNames.contains(Spark.platform().getClass.getName))
      Assert.assertTrue(deserializedPlatformNames.contains(Java.platform().getClass.getName))
    } catch {
      case t: Throwable =>
        t.printStackTrace()
        throw t
    }
  }

  @Test
  def targetPlatforms2Test(): Unit = {
    val configuration = new Configuration()
    val wayangContext = new WayangContext(configuration)
      .withPlugin(Java.basicPlugin())
    val planBuilder = new PlanBuilder(wayangContext)
      .withUdfJarsOf(classOf[OtherSerializationTests])

    val inputValues1 = Array("Big data is big.", "Is data big data?")
    val dataQuanta = planBuilder
      .loadCollection(inputValues1)
      .flatMap(_.split("\\s+"))
      .map(_.replaceAll("\\W+", "").toLowerCase)
      .map((_, 1))
      .reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2))
      .withTargetPlatforms(Spark.platform())

    try {
      val serialized = SerializationUtils.serializeAsString(dataQuanta.operator)
      val deserialized = SerializationUtils.deserializeFromString[Operator](serialized)
      Assert.assertEquals(deserialized.getTargetPlatforms.size(), 1)
      Assert.assertEquals(deserialized.getTargetPlatforms.toArray.toList(0).getClass.getName, Spark.platform().getClass.getName)
    } catch {
      case t: Throwable =>
        t.printStackTrace()
        throw t
    }
  }

}
