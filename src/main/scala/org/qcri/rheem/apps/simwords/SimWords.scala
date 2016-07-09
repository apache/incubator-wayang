package org.qcri.rheem.apps.simwords

import org.qcri.rheem.api._
import org.qcri.rheem.apps.util.Parameters
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval
import org.qcri.rheem.core.platform.Platform

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
    rheemCtx.getConfiguration.setProperty("rheem.core.optimizer.reoptimize", "false")
    platforms.foreach(rheemCtx.register)
    val planBuilder = new PlanBuilder(rheemCtx)

    // Create the word dictionary
    val _minWordOccurrences = minWordOccurrences
    val wordIds = planBuilder
      .readTextFile(inputFile).withName("Read corpus (1)")
      .flatMapJava(new ScrubFunction, selectivity = new ProbabilisticDoubleInterval(100, 10000, 0.9)).withName("Split & scrub")
      .map(word => (word, 1)).withName("Add word counter")
      .reduceByKey(_._1, (wc1, wc2) => (wc1._1, wc1._2 + wc2._2)).withName("Sum word counters")
      .withCardinalityEstimator((in: Long) => math.round(in * 0.01))
      .filter(_._2 >= _minWordOccurrences, selectivity = 1 - 20d / (20 + minWordOccurrences))
      .withName("Filter frequent words")
      .map(_._1).withName("Strip word counter")
      .zipWithId.withName("Zip with ID")
      .map(t => (t.field1, t.field0.toInt)).withName("Convert ID attachment")


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

    // Generate initial centroids.
    //    val initialCentroids = wordVectors
    //      .customOperator[(Int, SparseVector)](
    //      new SampleOperator[(Int, SparseVector)](numClusters, dataSetType[(Int, SparseVector)], SampleOperator.Methods.RANDOM)
    //    ).withName("Sample centroids")
    //      .map(x => x).withName("Identity (wa1)")
    val _numClusters = numClusters
    val initialCentroids = wordIds
      .map(_._2).withName("Strip words")
      .group().withName("Group IDs")
      .flatMap { ids =>
        import scala.collection.JavaConversions._
        val idArray = ids.toArray
        for (i <- 0 to _numClusters) yield (i, SparseVector.createRandom(idArray, .99, _numClusters))
      }.withName("Generate centroids")

    // Run k-means on the vectors.
    val finalCentroids = initialCentroids.repeat(numIterations, { centroids: DataQuanta[(Int, SparseVector)] =>
      val newCentroids: DataQuanta[(Int, SparseVector)] = wordVectors
        .mapJava(new SelectNearestCentroidFunction("centroids"), udfCpuLoad = (in1: Long, in2: Long, out: Long) => 100L * in1 * in2)
        .withBroadcast(centroids, "centroids")
        .withName("Select nearest centroids")
        .map(assignment => (assignment._3, assignment._2)).withName("Strip word ID")
        .reduceByKey(_._1, (wv1: (Int, SparseVector), wv2: (Int, SparseVector)) => (wv1._1, wv1._2 + wv2._2))
        .withName("Add up cluster words").withCardinalityEstimator((in: Long) => _numClusters.toLong)
        .map { centroid: (Int, SparseVector) => centroid._2.normalize(); centroid }.withName("Normalize centroids")

      newCentroids
    }).withName("K-means iteration").map(x => x).withName("Identity (wa2)")

    // Apply the centroids to the points and resolve the word IDs.
    val clusters = wordVectors
      .mapJava(new SelectNearestCentroidFunction("finalCentroids")).withBroadcast(finalCentroids, "finalCentroids").withName("Select nearest final centroids")
      .map(assigment => (assigment._3, List(assigment._1))).withName("Discard word vectors")
      .reduceByKey(_._1, (c1, c2) => (c1._1, c1._2 ++ c2._2)).withName("Create clusters")
      .map(_._2).withName("Discard cluster IDs")
      .mapJava(new ResolveClusterFunction("wordIds")).withBroadcast(wordIds, "wordIds").withName("Resolve word IDs")


    val result = clusters.withUdfJarsOf(classOf[SimWords]).collect(
      jobName = s"SimWords ($inputFile, reach=$neighborhoodReach, clusters=$numClusters, $numIterations iterations)"
    )


    result.toIndexedSeq.sortBy(_.size).reverse.take(100).foreach(println(_))

  }

}

object SimWords {

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println("Usage: <main class> <platform(,platform)*> <input file> <min word occurrences> <neighborhood reach> <#clusters> <#iterations>")
      sys.exit(1)
    }

    val platforms = Parameters.loadPlatforms(args(0), () => new Configuration)
    val inputFile = args(1)
    val minWordOccurrences = args(2).toInt
    val neighborhoodRead = args(3).toInt
    val numClusters = args(4).toInt
    val numIterations = args(5).toInt

    val simWords = new SimWords(platforms: _*)
    simWords(inputFile, minWordOccurrences, neighborhoodRead, numClusters, numIterations)
  }
}
