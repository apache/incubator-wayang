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

package org.apache.wayang.apps.simwords

import org.apache.wayang.core.function.ExecutionContext
import org.apache.wayang.core.function.FunctionDescriptor.ExtendedSerializableFunction
import org.apache.wayang.core.util.WayangCollections
import org.apache.logging.log4j.LogManager

import scala.collection.JavaConversions._
import scala.util.Random

/**
  * This functions keeps a set of centroids around and for each input word neighborhood vector, it assigns the closest
  * centroid.
  */
class SelectNearestCentroidFunction(broadcastName: String)
  extends ExtendedSerializableFunction[(Int, SparseVector), (Int, SparseVector, Int)] {

  private lazy val logger = LogManager.getLogger(getClass)

  private var centroids: java.util.List[(Int, SparseVector)] = _

  private lazy val random = new Random()

  override def open(executionCtx: ExecutionContext): Unit = {
    this.centroids = WayangCollections.asList(executionCtx.getBroadcast[(Int, SparseVector)](broadcastName))
  }

  override def apply(wnvector: (Int, SparseVector)): (Int, SparseVector, Int) = {
    var maxSimilarity = -1d
    var nearestCentroid: Int = -1
    this.centroids.foreach { centroid =>
      val similarity = math.abs(centroid._2 * wnvector._2)
      if (similarity > maxSimilarity) {
        maxSimilarity = similarity
        nearestCentroid = centroid._1
      }
    }

    if (nearestCentroid == -1) {
      logger.info("Did not find a matching centroid for {}", wnvector)
      maxSimilarity = 0
      nearestCentroid = this.centroids.get(this.random.nextInt(this.centroids.size()))._1
    }

    (wnvector._1, wnvector._2, nearestCentroid)
  }
}

