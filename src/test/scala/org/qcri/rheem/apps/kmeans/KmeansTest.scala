package org.qcri.rheem.apps.kmeans

import org.junit.Assert._
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
  def shouldWorkWithJava() = {
    val centroids = Kmeans.run(
      file = getTestFileUrl("kmeans-k4-10000.csv"),
      k = 4,
      numIterations = 100,
      JavaPlatform.getInstance
    )

    assertEquals(4, centroids.size)
    List(Point(-10, -10), Point(10, -10), Point(-10, 10), Point(10, 10)).foreach { expectedCentroid =>
      assertTrue(
        s"None of $centroids matches the expected centroid $expectedCentroid.",
        centroids.exists(centroid => centroid.distanceTo(expectedCentroid) < 6))
    }
  }

  @Test
  def shouldWorkWithSpark() = {
    val centroids = Kmeans.run(
      file = getTestFileUrl("kmeans-k4-10000.csv"),
      k = 4,
      numIterations = 100,
      SparkPlatform.getInstance
    )

    assertEquals(4, centroids.size)
    List(Point(-10, -10), Point(10, -10), Point(-10, 10), Point(10, 10)).foreach { expectedCentroid =>
      assertTrue(
        s"None of $centroids matches the expected centroid $expectedCentroid.",
        centroids.exists(centroid => centroid.distanceTo(expectedCentroid) < 6))
    }
  }

  @Test
  def shouldWorkWithJavaAndSpark() = {
    val centroids = Kmeans.run(
      file = getTestFileUrl("kmeans-k4-10000.csv"),
      k = 4,
      numIterations = 100,
      SparkPlatform.getInstance, JavaPlatform.getInstance
    )

    assertEquals(4, centroids.size)
    List(Point(-10, -10), Point(10, -10), Point(-10, 10), Point(10, 10)).foreach { expectedCentroid =>
      assertTrue(
        s"None of $centroids matches the expected centroid $expectedCentroid.",
        centroids.exists(centroid => centroid.distanceTo(expectedCentroid) < 6))
    }
  }
}

