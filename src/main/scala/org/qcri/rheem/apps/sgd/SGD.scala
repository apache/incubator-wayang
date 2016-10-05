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
        s"<dataset URL> <dataset size> <#features> <max iterations> <accuracy> <sample size>")
      sys.exit(1)
    }

    implicit val experiment = Parameters.createExperiment(args(0), this)
    implicit val configuration = new Configuration
    val plugins = Parameters.loadPlugins(args(1))
    experiment.getSubject.addConfiguration("plugins", args(1))
    val datasetUrl = args(2)
    experiment.getSubject.addConfiguration("input", datasetUrl)
    val datasetSize = args(3).toInt
    experiment.getSubject.addConfiguration("inputSize", datasetSize)
    val numFeatures = args(4).toInt
    experiment.getSubject.addConfiguration("features", numFeatures)
    val maxIterations = args(5).toInt
    experiment.getSubject.addConfiguration("maxIterations", maxIterations)
    val accuracy = args(6).toDouble
    experiment.getSubject.addConfiguration("accuracy", accuracy)
    val sampleSize = args(7).toInt
    experiment.getSubject.addConfiguration("sampleSize", sampleSize)

    // Initialize the SGD algorithm.
    val sgd = new SGDImpl(configuration, plugins.toArray)

    // Run the SGD.
    val weights = sgd(datasetUrl, datasetSize, numFeatures, maxIterations, accuracy, sampleSize)

    // Store experiment data.
    ProfileDBHelper.store(experiment, configuration)

    // Print the result.
    println(s"Determined weights: ${weights.map(w => f"$w%,.5f").mkString(", ")}")
  }

}
