package org.qcri.rheem.apps.simwords

import org.qcri.rheem.core.function.ExecutionContext
import org.qcri.rheem.core.function.FunctionDescriptor.ExtendedSerializableFunction
import org.qcri.rheem.core.util.RheemCollections
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.util.Random

/**
  * This functions keeps a set of centroids around and for each input word neighborhood vector, it assigns the closest
  * centroid.
  */
class SelectNearestCentroidFunction(broadcastName: String)
  extends ExtendedSerializableFunction[(Int, SparseVector), (Int, SparseVector, Int)] {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  private var centroids: java.util.List[(Int, SparseVector)] = _

  private lazy val random = new Random()

  override def open(executionCtx: ExecutionContext): Unit = {
    this.centroids = RheemCollections.asList(executionCtx.getBroadcast[(Int, SparseVector)](broadcastName))
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

