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

import org.apache.wayang.basic.operators.TextFileSink
import org.apache.wayang.core.api.WayangContext
import org.apache.wayang.core.plan.wayangplan.{LoopHeadOperator, Operator, WayangPlan}
import org.apache.wayang.core.util.ReflectionUtils
import org.junit.rules.TestName
import org.junit.{Assert, Rule}

import java.io.{File, FileWriter}
import java.nio.file.{Files, Paths}
import java.util.stream.Collectors
import scala.jdk.CollectionConverters.asScalaBufferConverter


trait SerializationTestBase {

  //
  // Some magic from https://stackoverflow.com/a/36152864/5589918 in order to get the current test name
  //
  var _testName: TestName = new TestName

  @Rule
  def testName: TestName = _testName

  def testName_=(aTestName: TestName): Unit = {
    _testName = aTestName
  }


  def serializeDeserializeExecuteAssert(operator: Operator, wayangContext: WayangContext, expectedLines: List[String], log: Boolean = false): Unit = {
    var tempFileOut: Option[String] = None
    try {
      tempFileOut = Some(serializeDeserializeExecute(operator, wayangContext, log)) // Serialize, deserialize, execute
      SerializationTestBase.assertOutputFile(tempFileOut.get, expectedLines) // Check results
    }
    catch {
      case t: Throwable =>
        t.printStackTrace()
        throw t
    }
    finally {
      // Delete tempFileOut if it exists
      tempFileOut match {
        case Some(file) => new java.io.File(file).delete()
        case None => // Do nothing
      }
    }
  }

  def serializeDeserializeExecute(operator: Operator, wayangContext: WayangContext, log: Boolean = false): String = {
    try {
      val serialized = SerializationUtils.serializeAsString(operator)
      if (log) SerializationTestBase.log(serialized, testName.getMethodName + ".log.json")
      val deserialized = SerializationUtils.deserializeFromString[Operator](serialized)

      // Create an output sink
      val outType = deserialized.getOutput(0).getType.getDataUnitType.getTypeClass
      val tempFilenameOut = s"/tmp/${testName.getMethodName}.out"
      val sink = new TextFileSink(s"file://$tempFilenameOut", outType)

      // And attach it to the deserialized operator
      deserialized match {
        case loopHeadOperator: LoopHeadOperator => loopHeadOperator.connectTo(1, sink, 0)
        case operator: Operator => operator.connectTo(0, sink, 0)
      }

      // Execute plan
      val plan = new WayangPlan(sink)
      wayangContext.execute(plan, ReflectionUtils.getDeclaringJar(classOf[OperatorSerializationTests]))
      tempFilenameOut
    }
    catch {
      case t: Throwable =>
        t.printStackTrace()
        throw t
    }
  }

}

object SerializationTestBase {

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

  def assertOutputFileLineCount(outputFilename: String, expectedNumberOfLines: Int): Unit = {

    // Read lines
    val lines = Files.lines(Paths.get(outputFilename)).collect(Collectors.toList[String]).asScala

    // Assert number of lines
    Assert.assertEquals("Number of lines in the file should match", expectedNumberOfLines, lines.size)
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