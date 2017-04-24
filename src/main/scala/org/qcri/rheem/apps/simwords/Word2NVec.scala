package org.qcri.rheem.apps.simwords

import de.hpi.isg.profiledb.store.model.Experiment
import org.qcri.rheem.api._
import org.qcri.rheem.apps.util.{ExperimentDescriptor, Parameters, ProfileDBHelper}
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.core.function.ExecutionContext
import org.qcri.rheem.core.function.FunctionDescriptor.ExtendedSerializableFunction
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators
import org.qcri.rheem.core.plugin.Plugin
import org.qcri.rheem.core.util.fs.FileSystems

/**
  * This app maps words from a corpus to vectors describing that words average neighborhood. The vector components
  * correspond to words and the values determine to some probability of finding that word in the given words neighborhood.
  *
  * <p>Note the UDF load properties `rheem.apps.simwords.udfs.create-neighborhood.load`.</p>
  */
class Word2NVec(plugins: Plugin*) {

  def apply(inputFile: String,
            minWordOccurrences: Int,
            neighborhoodReach: Int,
            wordsPerLine: ProbabilisticDoubleInterval,
            outputFile: String)
           (implicit experiment: Experiment,
            configuration: Configuration) = {

    // Initialize.
    val rheemCtx = new RheemContext(configuration)
    plugins.foreach(rheemCtx.register)
    val planBuilder = new PlanBuilder(rheemCtx)
      .withJobName(
        jobName = s"Word2NVec ($inputFile, reach=$neighborhoodReach, output=$outputFile)"
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

    // Enhance the word vectors by joining the actual word and write to an output file.
    wordVectors
      .mapJava(new ExtendWordVector)
      .withBroadcast(wordIds, "wordIds")
      .withName("Extend word vectors")
      .writeTextFile(outputFile, wv => s"${wv._1};${wv._2};${wv._3.toDictionaryString}")
  }

}

object Word2NVec extends ExperimentDescriptor {

  override def version = "0.1.0"

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println(s"Usage: <main class> ${Parameters.experimentHelp} <plugin(,plugin)*> <input file> <min word occurrences> <neighborhood reach> <output file> [<words per line (from..to)>]")
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
    val outputFile = args(5)
    experiment.getSubject.addConfiguration("output", outputFile)
    val wordsPerLine = if (args.length >= 7) {
      experiment.getSubject.addConfiguration("wordsPerLine", args(6))
      Parameters.parseAny(args(6)).asInstanceOf[ProbabilisticDoubleInterval]
    } else new ProbabilisticDoubleInterval(100, 10000, 0.9)

    val word2NVec = new Word2NVec(plugins: _*)
    word2NVec(inputFile, minWordOccurrences, neighborhoodRead, wordsPerLine, outputFile)

    // Store experiment data.
    val inputFileSize = FileSystems.getFileSize(inputFile)
    if (inputFileSize.isPresent) experiment.getSubject.addConfiguration("inputSize", inputFileSize.getAsLong)
    ProfileDBHelper.store(experiment, configuration)

  }
}

/**
  * Extend `(word ID, neighborhood vector)` elements to `(word ID, word, neighborhood vector)` elements.
  * <p>
  * Accepts broadcast `wordIds` of `(word, word ID)` pairs.
  */
private[simwords] class ExtendWordVector
  extends ExtendedSerializableFunction[(Int, SparseVector), (Int, String, SparseVector)] {

  private var words: Map[Int, String] = _

  /**
    * Called before this instance is actually executed.
    *
    * @param ctx the { @link ExecutionContext}
    */
  override def open(ctx: ExecutionContext): Unit = {
    import scala.collection.JavaConversions._
    this.words = ctx.getBroadcast[(String, Int)]("wordIds").map(_.swap).toMap
  }

  override def apply(t: (Int, SparseVector)): (Int, String, SparseVector) = (t._1, this.words.getOrElse(t._1, "(unknown)"), t._2)
}
