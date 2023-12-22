package org.apache.wayang.api.async

import org.apache.wayang.api.{BlossomContext, createPlanBuilder}
import org.apache.wayang.java.Java
import org.junit.jupiter.api.Assertions.fail
import org.junit.{Assert, Test}

import java.nio.file.{Files, Paths}
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class AsyncTest {

  @Test
  def testReadMapCollect(): Unit = {
    val tempFilePath = Paths.get("/tmp", "testReadMapCollect-output.txt")
    val wayang = new BlossomContext().withPlugin(Java.basicPlugin)
    val inputValues = (1 to 10).toArray
    val dq = wayang.loadCollection(inputValues).map(_ + 2)
    val resultFuture = runAsyncWithTempFileOut(dq, "file:///" + tempFilePath)

    try {
      // Await for the result of the future.
      val result = Await.result(resultFuture, 30.seconds)
      val output = wayang.readObjectFile[Int](result.tempFileOut).collect()
      val expectedOutputValues = List(3, 4, 5, 6, 7, 8, 9, 10, 11, 12)

      // Assert after the Await operation, so we're sure we are checking the completed value.
      Assert.assertEquals(output.toList, expectedOutputValues)
    } catch {
      // Catch any exception and fail the test providing the error message.
      case e: Exception => fail(s"An error occurred: ${e.getMessage}")
    } finally {
      Files.deleteIfExists(tempFilePath)
    }
  }

  @Test
  def testWordCount(): Unit = {
    val tempFilePath = Paths.get("/tmp", "testWordCount-output.txt")
    val wayang = new BlossomContext().withPlugin(Java.basicPlugin)
    val inputValues = Array("Big data is big.", "Is data big data?")
    val dq = wayang
      .loadCollection(inputValues).withName("Load input values")
      .flatMap(_.split("\\s+")).withName("Split words")
      .map(_.replaceAll("\\W+", "").toLowerCase).withName("To lowercase")
      .map((_, 1)).withName("Attach counter")
      .reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2)).withName("Sum counters")
    val resultFuture = runAsyncWithTempFileOut(dq, "file:///" + tempFilePath)

    try {
      // Await for the result of the future.
      val result = Await.result(resultFuture, 30.seconds)
      val output = wayang.readObjectFile[(String, Int)](result.tempFileOut).collect().toSet
      val expectedOutputValues = Set(("big", 3), ("is", 2), ("data", 4))

      // Assert after the Await operation, so we're sure we are checking the completed value.
      Assert.assertEquals(output, expectedOutputValues)
    } catch {
      // Catch any exception and fail the test providing the error message.
      case e: Exception => fail(s"An error occurred: ${e.getMessage}")
    } finally {
      Files.deleteIfExists(tempFilePath)
    }
  }

}