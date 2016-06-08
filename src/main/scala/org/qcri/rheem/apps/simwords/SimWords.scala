package org.qcri.rheem.apps.simwords

import org.qcri.rheem.api._
import org.qcri.rheem.basic.operators.SampleOperator
import org.qcri.rheem.core.api.RheemContext
import org.qcri.rheem.core.platform.Platform
import org.qcri.rheem.java.JavaPlatform
import org.qcri.rheem.spark.platform.SparkPlatform

/**
  * TODO
  */
class SimWords(platforms: Platform*) {

  def apply(inputFile: String,
            minWordOccurrences: Int,
            neighborhoodReach: Int,
            numClusters: Int,
            numIterations: Int) = {

    // Initialize.
    val rheemCtx = new RheemContext
    platforms.foreach(rheemCtx.register)
    val planBuilder = new PlanBuilder(rheemCtx)

    // Create the word dictionary
    val _minWordOccurrences = minWordOccurrences
    val wordIds = planBuilder
      .readTextFile(inputFile).withName("Read corpus (1)")
      .flatMapJava(new ScrubFunction).withName("Split & scrub")
      .map(word => (word, 1)).withName("Add word counter")
      .reduceByKey(_._1, (wc1, wc2) => (wc1._1, wc1._2 + wc2._2)).withName("Sum word counters")
      .filter(_._2 >= _minWordOccurrences).withName("Filter frequent words")
      .map(_._1).withName("Strip word counter")
      .mapJava(new AddIdFunction).withName("Add word ID")

    // Create the word neighborhood vectors.
    val wordVectors = planBuilder
      .readTextFile(inputFile).withName("Read corpus (2)")
      .flatMapJava(new CreateWordNeighborhoodFunction(neighborhoodReach, "wordIds"))
      .withBroadcast(wordIds, "wordIds")
      .withName("Create word vectors")
      .reduceByKey(_._1, (wv1, wv2) => (wv1._1, wv1._2 + wv2._2)).withName("Add word vectors")
      .map { wv =>
        wv._2.normalize(); wv
      }.withName("Normalize word vectors")

    // Sample initial centroids.
    val _numCentroids = numClusters
    val initialCentroids = wordVectors
//      .customOperator[(Int, SparseVector)](
//      new SampleOperator[(Int, SparseVector)](numClusters, dataSetType[(Int, SparseVector)], SampleOperator.Methods.RESERVOIR)
//    ).withName("Sample centroids")

    // Run k-means on the vectors.
    val finalCentroids = initialCentroids.repeat(numIterations, { centroids: DataQuanta[(Int, SparseVector)] =>
      val newCentroids: DataQuanta[(Int, SparseVector)] = wordVectors
        .mapJava(new SelectNearestCentroidFunction("centroids")).withBroadcast(centroids, "centroids").withName("Select nearest centroids")
        .map(assignment => (assignment._3, assignment._2))
        .reduceByKey(_._1, (wv1: (Int, SparseVector), wv2: (Int, SparseVector)) => (wv1._1, wv1._2 + wv2._2))
        .map { centroid: (Int, SparseVector) => centroid._2.normalize(); centroid }

      newCentroids
    }).withName("K-means iteration").map(x => x).withName("Identity (wa)")

    // Apply the centroids to the points and resolve the word IDs.
    val clusters = wordVectors
      .mapJava(new SelectNearestCentroidFunction("finalCentroids")).withBroadcast(finalCentroids, "finalCentroids").withName("Select nearest final centroids")
      .map(assigment => (assigment._3, List(assigment._1))).withName("Discard word vectors")
      .reduceByKey(_._1, (c1, c2) => (c1._1, c1._2 ++ c2._2)).withName("Create clusters")
      .map(_._2).withName("Discard cluster IDs")
      .mapJava(new ResolveClusterFunction("wordIds")).withBroadcast(wordIds, "wordIds").withName("Resolve word IDs")


    val result = clusters.withUdfJarsOf(classOf[SimWords]).collect()


    result.toIndexedSeq.sortBy(_.size).reverse.take(100).foreach(println(_))

  }

}

object SimWords {

  def main(args: Array[String]): Unit = {
    val simWords = new SimWords(JavaPlatform.getInstance, SparkPlatform.getInstance)
    simWords(
      inputFile = "file:///Users/basti/Work/Data/text/odyssey-squeezed.txt",
      minWordOccurrences = 2,
      neighborhoodReach = 2,
      numClusters = 200,
      numIterations = 10)
  }

}
