package org.qcri.rheem.tests

import org.junit.{Assert, Ignore, Test}
import org.qcri.rheem.api._
import org.qcri.rheem.core.api.RheemContext
import org.qcri.rheem.graphchi.GraphChiPlatform
import org.qcri.rheem.java.JavaPlatform
import org.qcri.rheem.postgres.PostgresPlatform
import org.qcri.rheem.spark.platform.SparkPlatform

/**
  * Tests based on the Scala API.
  */
class ScalaTest {

  @Test
  def testWordCount = {
    val rheem = new RheemContext
    rheem.register(JavaPlatform.getInstance)
    rheem.register(SparkPlatform.getInstance)
    rheem.register(GraphChiPlatform.getInstance)
    rheem.register(PostgresPlatform.getInstance)

    // Wikipedia abstract of Rheem, California ;)
    val text = Seq(
      "Rheem,[1] also known as Rheem Valley[1] and Rheem Center,[2] is an unincorporated community in Contra ",
      "Costa County, California, United States.[1] It is located 7.5 miles (12 km) north-northwest of ",
      "Danville,[2] at an elevation of 587 feet.",
      "The place was named after its developer, Donald Laird Rheem, the son of William Rheem, ",
      "President of Standard Oil Company.[2]"
    )


    val wordCounts = rheem
      .readCollection(text).withName("Load input values")
      .map(_.replaceAll("[^\\w\\s]+", " ").toLowerCase).withName("Scrub")
      .flatMap(_.split("\\s+")).withName("Split")
      .map((_, 1)).withName("Attach counter")
      .reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2)).withName("Sum counters")
      .collect().toSet

    val expectedWordCounts = Seq(("rheem", 5), ("is", 2), ("standard", 1))

    expectedWordCounts.foreach(wc => Assert.assertTrue(s"$wc not found.", wordCounts.contains(wc)))

  }

  @Ignore("TODO")
  @Test
  def testKMeans = {
    val rheem = new RheemContext
    rheem.register(JavaPlatform.getInstance)
    rheem.register(SparkPlatform.getInstance)

    // TODO
  }

}
