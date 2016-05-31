package org.qcri.rheem.api

import org.junit.{Assert, Test}
import org.qcri.rheem.core.function.ExecutionContext
import org.qcri.rheem.core.function.PredicateDescriptor.ExtendedSerializablePredicate
import org.qcri.rheem.java.JavaPlatform
import org.qcri.rheem.spark.platform.SparkPlatform

/**
  * Tests the Rheem API.
  */
class ApiTest {

  @Test
  def testReadMapCollect(): Unit = {
    // Set up Java platform.
    Rheem.rheemContext.register(JavaPlatform.getInstance)

    // Generate some test data.
    val inputValues = (for (i <- 1 to 10) yield i).toArray

    // Build and execute a Rheem plan.
    val outputValues = Rheem.buildNewPlan
      .readCollection(inputValues).withName("Load input values")
      .map(_ + 2).withName("Add 2")
      .collect()

    // Check the outcome.
    val expectedOutputValues = inputValues.map(_ + 2)
    Assert.assertArrayEquals(expectedOutputValues, outputValues.toArray)
  }

  @Test
  def testWordCount(): Unit = {
    // Set up RheemContext.
    Rheem.rheemContext.register(JavaPlatform.getInstance)
    Rheem.rheemContext.register(SparkPlatform.getInstance)

    // Generate some test data.
    val inputValues = Array("Big data is big.", "Is data big data?")

    // Build and execute a word count RheemPlan.
    val wordCounts = Rheem.buildNewPlan
      .readCollection(inputValues).withName("Load input values")
      .flatMap(_.split("\\s+")).withName("Split words")
      .map(_.replaceAll("\\W+", "").toLowerCase).withName("To lowercase")
      .map((_, 1)).withName("Attach counter")
      .reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2)).withName("Sum counters")
      .collect().toSet

    val expectedWordCounts = Set(("big", 3), ("is", 2), ("data", 3))

    Assert.assertEquals(expectedWordCounts, wordCounts)
  }

  @Test
  def testDoWhile(): Unit = {
    // Set up RheemContext.
    Rheem.rheemContext.register(JavaPlatform.getInstance)
    Rheem.rheemContext.register(SparkPlatform.getInstance)

    // Generate some test data.
    val inputValues = Array(1, 2)

    // Build and execute a word count RheemPlan.

    val values = Rheem.buildNewPlan
      .readCollection(inputValues).withName("Load input values")
      .doWhile[Int](vals => vals.max > 100, {
      start =>
        val sum = start.reduce(_ + _).withName("Sum")
        (start.union(sum).withName("Old+new"), sum.map(x => x).withName("Identity (hotfix)"))
    }).withName("While <= 100")
      .collect().toSet

    val expectedValues = Set(1, 2, 3, 6, 12, 24, 48, 96, 192)
    Assert.assertEquals(expectedValues, values)
  }

  @Test
  def testRepeat(): Unit = {
    // Set up RheemContext.
    Rheem.rheemContext.register(JavaPlatform.getInstance)
    Rheem.rheemContext.register(SparkPlatform.getInstance)

    // Generate some test data.
    val inputValues = Array(1, 2)

    // Build and execute a word count RheemPlan.

    val values = Rheem.buildNewPlan
      .readCollection(inputValues).withName("Load input values")
      .repeat(3,
        _.reduce(_ * _).withName("Multiply")
          .flatMap(v => Seq(v, v + 1)).withName("Duplicate")
      ).withName("Repeat 3x")
      .collect().toSet

    // initial: 1,2 -> 1st: 2,3 -> 2nd: 6,7 => 3rd: 42,43
    val expectedValues = Set(42, 43)
    Assert.assertEquals(expectedValues, values)
  }

  @Test
  def testBroadcast() = {
    // Set up RheemContext.
    Rheem.rheemContext.register(JavaPlatform.getInstance)
    Rheem.rheemContext.register(SparkPlatform.getInstance)

    // Generate some test data.
    val inputStrings = Array("Hello", "World", "Hi", "Mars")
    val selectors = Array('o', 'l')

    val builder = Rheem.buildNewPlan
    val selectorsDataSet = builder.readCollection(selectors).withName("Load selectors")

    // Build and execute a word count RheemPlan.
    val values = builder
      .readCollection(inputStrings).withName("Load input values")
      .filterJava(new ExtendedSerializablePredicate[String] {

        var selectors: Iterable[Char] = _

        override def open(ctx: ExecutionContext): Unit = {
          import scala.collection.JavaConversions._
          selectors = collectionAsScalaIterable(ctx.getBroadcast[Char]("selectors"))
        }

        override def test(t: String): Boolean = selectors.forall(selector => t.contains(selector))

      }).withName("Filter words")
      .withBroadcast(selectorsDataSet, "selectors")
      .collect().toSet

    val expectedValues = Set("Hello", "World")
    Assert.assertEquals(expectedValues, values)
  }

}
