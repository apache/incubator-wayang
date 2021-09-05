package org.qcri.rheem.apps.wordcount

import de.hpi.isg.profiledb.store.model.Experiment
import org.qcri.rheem.api._
import org.qcri.rheem.apps.util.{ExperimentDescriptor, Parameters, ProfileDBHelper}
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval
import org.qcri.rheem.core.plugin.Plugin
import org.qcri.rheem.core.util.fs.FileSystems

/**
  * This is app counts words in a file.
  *
  * @see [[org.qcri.rheem.apps.wordcount.Main]]
  */
class WordCountScala(plugin: Plugin*) {

  /**
    * Run the word count over a given file.
    *
    * @param inputUrl     URL to the file
    * @param wordsPerLine optional estimate of how many words there are in each line
    * @return the counted words
    */
  def apply(inputUrl: String,
            wordsPerLine: ProbabilisticDoubleInterval = new ProbabilisticDoubleInterval(100, 10000, .8d))
           (implicit configuration: Configuration, experiment: Experiment) = {
    val rheemCtx = new RheemContext(configuration)
    plugin.foreach(rheemCtx.register)
    val planBuilder = new PlanBuilder(rheemCtx)

    planBuilder
      .withJobName(s"WordCount ($inputUrl)")
      .withExperiment(experiment)
      .withUdfJarsOf(this.getClass)
      .readTextFile(inputUrl).withName("Load file")
      .flatMap(_.split("\\W+"), selectivity = wordsPerLine).withName("Split words")
      .filter(_.nonEmpty, selectivity = 0.99).withName("Filter empty words")
      .map(word => (word.toLowerCase, 1)).withName("To lower case, add counter")
      .reduceByKey(_._1, (c1, c2) => (c1._1, c1._2 + c2._2)).withName("Add counters")
      .withCardinalityEstimator((in: Long) => math.round(in * 0.01))
      .collect()
  }

}

/**
  * Companion object for [[WordCountScala]].
  */
object WordCountScala extends ExperimentDescriptor {

  override def version = "0.1.0"

  def main(args: Array[String]) {
    // Parse args.
    if (args.isEmpty) {
      println(s"Usage: <main class> ${Parameters.experimentHelp} <plugin(,plugin)*> <input file> [<words per line a..b[~confidence]>]")
      sys.exit(1)
    }
    implicit val configuration = new Configuration
    implicit val experiment = Parameters.createExperiment(args(0), this)
    val plugins = Parameters.loadPlugins(args(1))
    experiment.getSubject.addConfiguration("plugins", args(1))
    val inputFile = args(2)
    experiment.getSubject.addConfiguration("input", inputFile)
    val wordsPerLine = if (args.length >= 4) {
      experiment.getSubject.addConfiguration("wordsPerLine", args(3))
      Parameters.parseAny(args(3)).asInstanceOf[ProbabilisticDoubleInterval]
    } else null

    // Run wordCount.
    val wordCount = new WordCountScala(plugins: _*)
    val words =
      (if (wordsPerLine != null) {
        wordCount(inputFile, wordsPerLine)
      } else {
        wordCount(inputFile)
      }).toSeq.sortBy(-_._2)

    // Store experiment data.
    val inputFileSize = FileSystems.getFileSize(inputFile)
    if (inputFileSize.isPresent) experiment.getSubject.addConfiguration("inputSize", inputFileSize.getAsLong)
    ProfileDBHelper.store(experiment, configuration)

    // Print results.
    println(s"Found ${words.size} words:")
    words.take(10).foreach(wc => println(s"${wc._2}x ${wc._1}"))
    if (words.size > 10) print(s"${words.size - 10} more...")
  }

}
