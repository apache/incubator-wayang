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

package org.apache.wayang.apps.wordcount

import org.apache.wayang.apps.util.{ExperimentDescriptor, Parameters, ProfileDBHelper}
import org.apache.wayang.api._
import org.apache.wayang.apps.util.ProfileDBHelper
import org.apache.wayang.commons.util.profiledb.model.Experiment
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval
import org.apache.wayang.core.plugin.Plugin
import org.apache.wayang.core.util.fs.FileSystems

/**
  * This is app counts words in a file.
  *
  * @see [[WordCountWithJavaNativeAPI]]
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
    val wayangCtx = new WayangContext(configuration)
    plugin.foreach(wayangCtx.register)
    val planBuilder = new PlanBuilder(wayangCtx)

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
