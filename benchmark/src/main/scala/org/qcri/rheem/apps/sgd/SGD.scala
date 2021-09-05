package org.qcri.rheem.apps.sgd

import org.qcri.rheem.apps.util.{ExperimentDescriptor, Parameters, ProfileDBHelper}
import org.qcri.rheem.core.api.Configuration

/**
  * Companion for the [[SGDImpl]] class.
  */
object SGD extends ExperimentDescriptor {

  override def version = "1.0"

  def main(args: Array[String]): Unit = {
    // Parse args.
    if (args.isEmpty) {
      println(s"Usage: scala <main class> ${Parameters.experimentHelp} <plugin(,plugin)*> " +
        s"<aggregation (regular|preaggregation)> <dataset URL> <dataset size> <#features> <max iterations> <accuracy> <sample size>")
      sys.exit(1)
    }

    implicit val experiment = Parameters.createExperiment(args(0), this)
    implicit val configuration = new Configuration
    val plugins = Parameters.loadPlugins(args(1))
    experiment.getSubject.addConfiguration("plugins", args(1))
    val aggregationType = args(2)
    experiment.getSubject.addConfiguration("aggregationType", aggregationType)
    val datasetUrl = args(3)
    experiment.getSubject.addConfiguration("input", datasetUrl)
    val datasetSize = args(4).toInt
    experiment.getSubject.addConfiguration("inputSize", datasetSize)
    val numFeatures = args(5).toInt
    experiment.getSubject.addConfiguration("features", numFeatures)
    val maxIterations = args(6).toInt
    experiment.getSubject.addConfiguration("maxIterations", maxIterations)
    val accuracy = args(7).toDouble
    experiment.getSubject.addConfiguration("accuracy", accuracy)
    val sampleSize = args(8).toInt
    experiment.getSubject.addConfiguration("sampleSize", sampleSize)

    var weights: Array[Double] = null
    aggregationType match {
      case "regular" =>
        // Initialize the SGD algorithm.
        val sgd = new SGDImpl(configuration, plugins.toArray)
        // Run the SGD.
        weights = sgd(datasetUrl, datasetSize, numFeatures, maxIterations, accuracy, sampleSize, experiment)
      case "preaggregation" =>
        // Initialize the SGD algorithm.
        val sgd = new SGDImprovedImpl(configuration, plugins.toArray)
        // Run the SGD.
        weights = sgd(datasetUrl, datasetSize, numFeatures, maxIterations, accuracy, sampleSize, experiment)
      case other => sys.error("Unknown aggregation type: " + other)
    }

    // Store experiment data.
    ProfileDBHelper.store(experiment, configuration)

    // Print the result.
    if (weights != null) println(s"Determined weights: ${weights.map(w => f"$w%,.5f").mkString(", ")}")
  }

}
