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

import org.apache.wayang.api.serialization.SerializationUtils
import org.apache.wayang.basic.operators.TextFileSink
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.plan.wayangplan.{Operator, WayangPlan}
import org.apache.wayang.core.util.ReflectionUtils
import org.apache.wayang.java.Java
import org.apache.wayang.spark.Spark
import org.junit.rules.TestName
import org.junit.{Assert, Rule, Test}

import java.io.{File, FileWriter}
import java.nio.file.{Files, Paths}
import java.util.stream.Collectors
import scala.jdk.CollectionConverters.asScalaBufferConverter

class SerializationUtilsTest {

  //
  // Some magic from https://stackoverflow.com/a/36152864/5589918 in order to get the current test name
  //
  var _testName: TestName = new TestName

  @Rule
  def testName: TestName = _testName

  def testName_=(aTestName: TestName): Unit = {
    _testName = aTestName
  }


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
      log(SerializationUtils.serializeAsString(deserialized), testName.getMethodName + ".log.json")
      Assert.assertEquals(
        multiContextPlanBuilder.withClassesOf,
        deserialized.withClassesOf
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


//  @Test
  def operatorSerializationTest(): Unit = {

    // Define configuration
    val configuration = new Configuration()
    val wayangContext = new WayangContext(configuration)
      .withPlugin(Java.basicPlugin())
    val planBuilder = new PlanBuilder(wayangContext)
      .withUdfJarsOf(classOf[SerializationUtilsTest])

    // Define plan
    val dataQuanta = planBuilder
      .loadCollection(List("12345", "12345678", "1234567890", "1234567890123"))
      .map(s => s + " Wayang out")
      .map(s => (s, "AAAA", "BBBB"))
      .map(s => List(s._1, "a", "b", "c"))
      .filter(s => s.head.length > 20)
      .map(s => s.head)

    try {
      val serialized = SerializationUtils.serialize(dataQuanta.operator) // serialize
//      log(serialized, testName.getMethodName + ".log.json") // log
      val deserialized = SerializationUtils.deserialize[Operator](serialized) // deserialize

      // Attach an output sink to deserialized plan
      val tempFileOut = s"/tmp/${testName.getMethodName}.out"
      val sink = new TextFileSink[AnyRef](s"file://$tempFileOut", classOf[AnyRef])
      deserialized.connectTo(0, sink, 0)

      // Execute plan
      val plan = new WayangPlan(sink)
      wayangContext.execute(plan, ReflectionUtils.getDeclaringJar(classOf[SerializationUtilsTest]))

      // Check results
      val expectedLines = List("1234567890 Wayang out", "1234567890123 Wayang out")
      assertOutputFile(tempFileOut, expectedLines)
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
      .withUdfJarsOf(classOf[SerializationUtilsTest])

    // Define plan
    val dataQuanta = planBuilder
      .loadCollection(List("12345", "12345678", "1234567890", "1234567890123"))
      .map(s => s + " Wayang out")
      .map(s => (s, "AAAA", "BBBB"))
      .map(s => List(s._1, "a", "b", "c"))
      .filter(s => s.head.length > 20)
      .map(s => s.head)

    val tempfile = MultiContextDataQuanta.writeToTempFile2(dataQuanta.operator)
    val operator = MultiContextDataQuanta.readFromTempFile2[Operator](tempfile)

    // Attach an output sink to deserialized plan
    val tempFileOut = s"/tmp/${testName.getMethodName}.out"
    val sink = new TextFileSink[AnyRef](s"file://$tempFileOut", classOf[AnyRef])
    operator.connectTo(0, sink, 0)

    // Execute plan
    val plan = new WayangPlan(sink)
    wayangContext.execute(plan, ReflectionUtils.getDeclaringJar(classOf[SerializationUtilsTest]))

    // Check results
    val expectedLines = List("1234567890 Wayang out", "1234567890123 Wayang out")
    assertOutputFile(tempFileOut, expectedLines)
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
      val multiContextPlanBuilder = MultiContextPlanBuilder(List(context1, context2))
        .withUdfJarsOf(classOf[SerializationUtilsTest])

      // Build and execute plan
      multiContextPlanBuilder
        .loadCollection(List("aaabbb", "aaabbbccc", "aaabbbcccddd", "aaabbbcccdddeee"))
        .map(s => s + " Wayang out.")
//        .filter(s => s.length > 20)
        .execute()

      // Check results
      val expectedLines = List("aaabbbcccddd Wayang out", "aaabbbcccdddeee Wayang out")
      assertOutputFile(out1, expectedLines)
      assertOutputFile(out2, expectedLines)
    }
    catch {
      case t: Throwable =>
        t.printStackTrace()
        throw t
    }
  }


  def assertOutputFile(outputFilename: String, expectedLines: List[String]): Unit = {

    // Read lines
    val lines = Files.lines(Paths.get(outputFilename)).collect(Collectors.toList[String]).asScala

    // Assert number of lines
    Assert.assertEquals("Number of lines in the file should match", expectedLines.size, lines.size)

    // Assert content of lines
    lines.zip(expectedLines).foreach { case (actual, expected) =>
      Assert.assertEquals("Line content should match", expected, actual)
    }
  }


  def log(text: String, filename: String = "customLogFile.json"): Unit = {

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

    // Log the path of the temporary file
    println(s"Temp file created at: ${customFile.getAbsolutePath}")
  }

}
