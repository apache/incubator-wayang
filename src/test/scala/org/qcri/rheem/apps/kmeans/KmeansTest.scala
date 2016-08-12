package org.qcri.rheem.apps.kmeans

import de.hpi.isg.profiledb.store.model.{Experiment, Subject}
import org.junit.Assert._
import org.junit.Test
import org.qcri.rheem.core.api.Configuration
import org.qcri.rheem.java.Java
import org.qcri.rheem.spark.Spark

/**
  * Test suite for [[Kmeans]].
  */
class KmeansTest {

  implicit val experiment = new Experiment("test", new Subject("test", "23.42"))

  implicit val configuration = new Configuration

  private def getTestFileUrl(fileName: String) =
    Thread.currentThread().getContextClassLoader.getResource(fileName).toString


  @Test
  def shouldWorkWithJava() = {
    val kmeans = new Kmeans(Java.basicPlugin)
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
    val kmeans = new Kmeans(Spark.basicPlugin)
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
    val kmeans = new Kmeans(Java.basicPlugin, Spark.basicPlugin)
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

