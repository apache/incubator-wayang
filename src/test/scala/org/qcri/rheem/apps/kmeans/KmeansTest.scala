package org.qcri.rheem.apps.kmeans

import org.junit.Test
import org.qcri.rheem.java.JavaPlatform
import org.qcri.rheem.spark.platform.SparkPlatform

/**
  * Test suite for [[Kmeans]].
  */
class KmeansTest {

  private def getTestFileUrl(fileName: String) =
    Thread.currentThread().getContextClassLoader.getResource(fileName).toString


  @Test
  def shouldWorkWithJava = {
    val points = Kmeans.run(
      file = getTestFileUrl("points-k4.csv"),
      k = 4,
      numIterations = 50,
      JavaPlatform.getInstance
    )
    println(points)
  }

  @Test
  def shouldWorkWithSpark = {
    val points = Kmeans.run(
      file = getTestFileUrl("points-k4.csv"),
      k = 4,
      numIterations = 50,
      SparkPlatform.getInstance
    )
    println(points)
  }

  @Test
  def shouldWorkWithJavaAndSpark = {
    val points = Kmeans.run(
      file = getTestFileUrl("points-k4.csv"),
      k = 4,
      numIterations = 50,
      SparkPlatform.getInstance, JavaPlatform.getInstance
    )
    println(points)
  }

}
