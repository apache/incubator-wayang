package org.qcri.rheem.apps.wordcount

import org.qcri.rheem.core.api.RheemContext
import org.qcri.rheem.core.platform.Platform
import org.qcri.rheem.api._
import org.qcri.rheem.java.JavaPlatform
import org.qcri.rheem.spark.platform.SparkPlatform

/**
  * This is app counts words in a file.
  *
  * @see [[org.qcri.rheem.apps.wordcount.Main]]
  */
class WordCountScala(platforms: Platform*) {

  /**
    * Run the word count over a given file.
    * @param inputUrl URL to the file
    * @return the counted words
    */
  def apply(inputUrl: String) = {
    val rheemCtx = new RheemContext
    platforms.foreach(rheemCtx.register)

    rheemCtx
      .readTextFile(inputUrl).withName("Load file")
      .flatMap(_.split("\\W+")).withName("Split words")
      .filter(_.nonEmpty).withName("Filter empty words")
      .map(word => (word.toLowerCase, 1)).withName("To lower case, add counter")
      .reduceByKey(_._1, (c1, c2) => (c1._1, c1._2 + c2._2)).withName("Add counters")
      .collect()
  }

}

/**
  * Companion object for [[WordCountScala]].
  */
object WordCountScala {

  def main(args: Array[String]) {
    if (args.isEmpty) {
      println("Usage: <main class> <platform(,platform)*> <input file>")
      sys.exit(1)
    }

    val platforms = args(0).split(",").map {
      case "spark" => SparkPlatform.getInstance
      case "java" => JavaPlatform.getInstance
      case misc => sys.error(s"Unknown platform: $misc")
    }.toSeq

    val inputFile = args(1)

    val wordCount = new WordCountScala(platforms: _*)
    val words = wordCount(inputFile).toSeq.sortBy(-_._2)

    println(s"Found ${words.size} words:")
    words.foreach(wc => println(s"${wc._2}x ${wc._1}"))
  }

}
