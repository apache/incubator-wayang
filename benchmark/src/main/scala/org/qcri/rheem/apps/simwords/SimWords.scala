package org.qcri.rheem.apps.simwords

import de.hpi.isg.profiledb.store.model.Experiment
import org.qcri.rheem.api._
import org.qcri.rheem.apps.util.{ExperimentDescriptor, Parameters, ProfileDBHelper}
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators
import org.qcri.rheem.core.plugin.Plugin
import org.qcri.rheem.core.util.fs.FileSystems

/**
  * This app clusters words by their word neighborhoods in a corpus.
  * <p>Note the UDF load properties `rheem.apps.simwords.udfs.create-neighborhood.load` and `rheem.apps.simwords.udfs.select-centroid.load`.</p>
  */
class SimWords(plugins: Plugin*) {

  def apply(inputFile: String,
            minWordOccurrences: Int,
            neighborhoodReach: Int,
            numClusters: Int,
            numIterations: Int,
            wordsPerLine: ProbabilisticDoubleInterval)
           (implicit experiment: Experiment,
            configuration: Configuration) = {

    // Initialize.
    val rheemCtx = new RheemContext(configuration)
    plugins.foreach(rheemCtx.register)
    val planBuilder = new PlanBuilder(rheemCtx)
      .withJobName(
        jobName = s"SimWords ($inputFile, reach=$neighborhoodReach, clusters=$numClusters, $numIterations iterations)"
      ).withExperiment(experiment)
      .withUdfJarsOf(this.getClass)

    // Create the word dictionary
    val _minWordOccurrences = minWordOccurrences
    val wordIds = planBuilder
      .readTextFile(inputFile).withName("Read corpus (1)")
      .flatMapJava(new ScrubFunction, selectivity = wordsPerLine).withName("Split & scrub")
      .map(word => (word, 1)).withName("Add word counter")
      .reduceByKey(_._1, (wc1, wc2) => (wc1._1, wc1._2 + wc2._2)).withName("Sum word counters")
      .withCardinalityEstimator((in: Long) => math.round(in * 0.01))
      .filter(_._2 >= _minWordOccurrences, selectivity = 10d / (9d + minWordOccurrences))
      .withName("Filter frequent words")
      .map(_._1).withName("Strip word counter")
      .zipWithId.withName("Zip with ID")
      .map(t => (t.field1, t.field0.toInt)).withName("Convert ID attachment")


    // Create the word neighborhood vectors.
    val wordVectors = planBuilder
      .readTextFile(inputFile).withName("Read corpus (2)")
      .flatMapJava(
        new CreateWordNeighborhoodFunction(neighborhoodReach, "wordIds"),
        selectivity = wordsPerLine,
        udfLoad = LoadProfileEstimators.createFromSpecification("rheem.apps.simwords.udfs.create-neighborhood.load", configuration)

      )
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
        .mapJava(
          new SelectNearestCentroidFunction("centroids"),
          udfLoad = LoadProfileEstimators.createFromSpecification("rheem.apps.simwords.udfs.select-centroid.load", configuration)
        )
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


    clusters.collect()
  }

}

object SimWords extends ExperimentDescriptor {

  override def version = "0.1.0"

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println(s"Usage: <main class> ${Parameters.experimentHelp} <plugin(,plugin)*> <input file> <min word occurrences> <neighborhood reach> <#clusters> <#iterations> [<words per line (from..to)>]")
      sys.exit(1)
    }

    implicit val configuration = new Configuration
    implicit val experiment = Parameters.createExperiment(args(0), this)
    val plugins = Parameters.loadPlugins(args(1))
    experiment.getSubject.addConfiguration("plugins", args(1))
    val inputFile = args(2)
    experiment.getSubject.addConfiguration("input", args(2))
    val minWordOccurrences = args(3).toInt
    experiment.getSubject.addConfiguration("minWordOccurrences", args(3))
    val neighborhoodRead = args(4).toInt
    experiment.getSubject.addConfiguration("neighborhoodReach", args(4))
    val numClusters = args(5).toInt
    experiment.getSubject.addConfiguration("clusters", args(5))
    val numIterations = args(6).toInt
    experiment.getSubject.addConfiguration("iterations", args(6))
    val wordsPerLine = if (args.length >= 8) {
      experiment.getSubject.addConfiguration("wordsPerLine", args(7))
      Parameters.parseAny(args(7)).asInstanceOf[ProbabilisticDoubleInterval]
    } else new ProbabilisticDoubleInterval(100, 10000, 0.9)

    val simWords = new SimWords(plugins: _*)
    val result = simWords(inputFile, minWordOccurrences, neighborhoodRead, numClusters, numIterations, wordsPerLine)

    // Store experiment data.
    val inputFileSize = FileSystems.getFileSize(inputFile)
    if (inputFileSize.isPresent) experiment.getSubject.addConfiguration("inputSize", inputFileSize.getAsLong)
    ProfileDBHelper.store(experiment, configuration)

    // Print the results.
    result.filter(_.size > 1).toIndexedSeq.sortBy(_.size).reverse.foreach(println(_))
  }
}
