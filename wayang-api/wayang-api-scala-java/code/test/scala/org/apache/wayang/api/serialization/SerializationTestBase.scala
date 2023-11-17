package org.apache.wayang.api.serialization

import org.apache.wayang.basic.operators.TextFileSink
import org.apache.wayang.core.api.WayangContext
import org.apache.wayang.core.plan.wayangplan.{Operator, WayangPlan}
import org.apache.wayang.core.util.ReflectionUtils
import org.junit.{Assert, Rule}
import org.junit.rules.TestName

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


  def serializeThenDeserializeThenAssertOutput(operator: Operator, wayangContext: WayangContext, expectedLines: List[String]): Unit = {
    var tempFileOut: Option[String] = None
    try {
      val serialized = SerializationUtils.serialize(operator)
      // log(serialized, testName.getMethodName + ".log.json")
      val deserialized = SerializationUtils.deserialize[Operator](serialized)

      // Attach an output sink to deserialized operator
      val outType = deserialized.getOutput(0).getType.getDataUnitType.getTypeClass
      tempFileOut = Some(s"/tmp/${testName.getMethodName}.out")
      val sink = new TextFileSink(s"file://${tempFileOut.get}", outType)
      deserialized.connectTo(0, sink, 0)

      // Execute plan
      val plan = new WayangPlan(sink)
      wayangContext.execute(plan, ReflectionUtils.getDeclaringJar(classOf[OperatorSerializationTests]))

      // Check results
      SerializationTestBase.assertOutputFile(tempFileOut.get, expectedLines)
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