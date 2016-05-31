package org.qcri.rheem.api

import org.junit.{Assert, Test}
import org.qcri.rheem.java.JavaPlatform
import org.qcri.rheem.spark.platform.SparkPlatform

/**
  * Tests the Rheem API.
  */
class ApiTest {

  @Test
  def testReadMapCollect(): Unit = {
    // Set up Java platform.
    Rheem.rheemContext.register(JavaPlatform.getInstance)

    // Generate some test data.
    val inputValues = (for (i <- 1 to 10) yield i).toArray

    // Build and execute a Rheem plan.
    val outputValues = Rheem.buildNewPlan
      .readCollection(inputValues)
      .map(_ + 2)
      .collect()

    // Check the outcome.
    val expectedOutputValues = inputValues.map(_ + 2)
    Assert.assertArrayEquals(expectedOutputValues, outputValues.toArray)
  }

  @Test
  def testWordCount(): Unit = {
    // Set up RheemContext.
    Rheem.rheemContext.register(JavaPlatform.getInstance)
    Rheem.rheemContext.register(SparkPlatform.getInstance)

    // Generate some test data.
    val inputValues = Array("Big data is big.", "Is data big data?")

    // Build and execute a word count RheemPlan.
    val wordCounts = Rheem.buildNewPlan
      .readCollection(inputValues)
      .flatMap(_.split("\\s+"))
      .map(_.replaceAll("\\W+", "").toLowerCase)
      .map((_, 1))
      .reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2))
      .collect().toSet

    val expectedWordCounts = Set(("big", 3), ("is", 2), ("data", 3))

    Assert.assertEquals(expectedWordCounts, wordCounts)
  }

  @Test
  def testDoWhile(): Unit = {
    // Set up RheemContext.
    Rheem.rheemContext.register(JavaPlatform.getInstance)
    Rheem.rheemContext.register(SparkPlatform.getInstance)

    // Generate some test data.
    val inputValues = Array(1, 2)

    // Build and execute a word count RheemPlan.

    val values = Rheem.buildNewPlan
      .readCollection(inputValues)
      .doWhile[Int](vals => vals.max > 100, {
      start =>
        val sum = start.reduce(_ + _)
        (start.union(sum), sum.map(x => x))
    })
      .collect().toSet

    val expectedValues = Set(1, 2, 3, 6, 12, 24, 48, 96, 192)
    Assert.assertEquals(expectedValues, values)
  }

}
