package org.apache.wayang.multicontext.apps.wordcount

import org.apache.wayang.api.{BlossomContext, MultiContextPlanBuilder}
import org.apache.wayang.java.Java
import org.apache.wayang.multicontext.apps.loadConfig
import org.apache.wayang.spark.Spark

class WordCountWithTargetPlatforms {}

object WordCountWithTargetPlatforms {

  def main(args: Array[String]): Unit = {
    println("Counting words in multi context wayang!")
    println("Scala version:")
    println(scala.util.Properties.versionString)

    val (configuration1, configuration2) = loadConfig(args)

    val context1 = new BlossomContext(configuration1)
      .withPlugin(Java.basicPlugin())
      .withPlugin(Spark.basicPlugin())
      .withTextFileSink("file:///tmp/out11")
    val context2 = new BlossomContext(configuration2)
      .withPlugin(Java.basicPlugin())
      .withPlugin(Spark.basicPlugin())
      .withTextFileSink("file:///tmp/out12")

    val multiContextPlanBuilder = MultiContextPlanBuilder(List(context1, context2))
      .withUdfJarsOf(classOf[WordCount])

    // Generate some test data
    val inputValues1 = Array("Big data is big.", "Is data big data?")
    val inputValues2 = Array("Big big data is big big.", "Is data big data big?")

    // Build and execute a word count
    multiContextPlanBuilder
      .loadCollection(context1, inputValues1)
      .loadCollection(context2, inputValues2)

      .flatMap(_.split("\\s+"))
      .withTargetPlatforms(context1, Spark.platform())
      .withTargetPlatforms(context2, Java.platform())

      .map(_.replaceAll("\\W+", "").toLowerCase)
      .withTargetPlatforms(Java.platform())

      .map((_, 1))

      .reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2))
      .withTargetPlatforms(context1, Spark.platform())

      .execute()
  }
}