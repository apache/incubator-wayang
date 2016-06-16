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
    val kmeans = new Kmeans(JavaPlatform.getInstance)
    val centroids = kmeans(
      k = 4,
      inputFile = getTestFileUrl("kmeans-k4-10000.csv"),
      iterations = 100,
      isResurrect = true
    )

    assertEquals(4, centroids.size)
//    List(Point(-10, -10), Point(10, -10), Point(-10, 10), Point(10, 10)).foreach { expectedCentroid =>
//      assertTrue(
//        s"None of $centroids matches the expected centroid $expectedCentroid.",
//        centroids.exists(centroid => centroid.distanceTo(expectedCentroid) < 6))
//    }
  }

  @Test
  def shouldWorkWithSpark() = {
    val kmeans = new Kmeans(SparkPlatform.getInstance)
    val centroids = kmeans(
      k = 4,
      inputFile = getTestFileUrl("kmeans-k4-10000.csv"),
      iterations = 100,
      isResurrect = true
    )

    assertEquals(4, centroids.size)
//    List(Point(-10, -10), Point(10, -10), Point(-10, 10), Point(10, 10)).foreach { expectedCentroid =>
//      assertTrue(
//        s"None of $centroids matches the expected centroid $expectedCentroid.",
//        centroids.exists(centroid => centroid.distanceTo(expectedCentroid) < 6))
//    }
  }

  @Test
  def shouldWorkWithJavaAndSpark() = {
    val kmeans = new Kmeans(JavaPlatform.getInstance, SparkPlatform.getInstance)
    val centroids = kmeans(
      k = 4,
      inputFile = getTestFileUrl("kmeans-k4-10000.csv"),
      iterations = 100,
      isResurrect = true
    )

    assertEquals(4, centroids.size)
//    List(Point(-10, -10), Point(10, -10), Point(-10, 10), Point(10, 10)).foreach { expectedCentroid =>
//      assertTrue(
//        s"None of $centroids matches the expected centroid $expectedCentroid.",
//        centroids.exists(centroid => centroid.distanceTo(expectedCentroid) < 6))
//    }
  }
}

