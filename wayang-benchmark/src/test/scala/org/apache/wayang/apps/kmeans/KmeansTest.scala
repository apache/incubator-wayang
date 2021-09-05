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

package org.apache.wayang.apps.kmeans

import org.apache.wayang.commons.util.profiledb.model.{Experiment, Subject}
import org.junit.Assert._
import org.junit.Test
import org.apache.wayang.core.api.Configuration
import org.apache.wayang.java.Java
import org.apache.wayang.spark.Spark

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
      inputFile = getTestFileUrl("kmeans-k4-10000.input"),
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
      inputFile = getTestFileUrl("kmeans-k4-10000.input"),
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
      inputFile = getTestFileUrl("kmeans-k4-10000.input"),
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

