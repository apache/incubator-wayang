package org.qcri.rheem.api

import org.junit.{Assert, Test}
import org.qcri.rheem.java.JavaPlatform

/**
  * Tests the Rheem API.
  */
class ApiTest {

  @Test
  def test(): Unit = {
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

}
