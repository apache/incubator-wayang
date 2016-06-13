package org.qcri.rheem.api

import org.junit.{Assert, Test}
import org.qcri.rheem.core.api.RheemContext
import org.qcri.rheem.core.function.PredicateDescriptor.ExtendedSerializablePredicate
import org.qcri.rheem.core.function.{ExecutionContext, TransformationDescriptor}
import org.qcri.rheem.java.JavaPlatform
import org.qcri.rheem.java.operators.JavaMapOperator
import org.qcri.rheem.spark.platform.SparkPlatform

/**
  * Tests the Rheem API.
  */
class ApiTest {

  @Test
  def testReadMapCollect(): Unit = {
    // Set up RheemContext.
    val rheem = new RheemContext()
    rheem.register(JavaPlatform.getInstance)
    rheem.register(SparkPlatform.getInstance)

    // Generate some test data.
    val inputValues = (for (i <- 1 to 10) yield i).toArray

    // Build and execute a Rheem plan.
    val outputValues = rheem
      .readCollection(inputValues).withName("Load input values")
      .map(_ + 2).withName("Add 2")
      .collect()

    // Check the outcome.
    val expectedOutputValues = inputValues.map(_ + 2)
    Assert.assertArrayEquals(expectedOutputValues, outputValues.toArray)
  }

  @Test
  def testCustomOperator(): Unit = {
    // Set up RheemContext.
    val rheem = new RheemContext()
    rheem.register(JavaPlatform.getInstance)
    rheem.register(SparkPlatform.getInstance)

    // Generate some test data.
    val inputValues = (for (i <- 1 to 10) yield i).toArray

    // Build and execute a Rheem plan.
    val inputDataSet = rheem.readCollection(inputValues).withName("Load input values")

    // Add the custom operator.
    val IndexedSeq(addedValues) = rheem.customOperator(new JavaMapOperator(
      dataSetType[Int],
      dataSetType[Int],
      new TransformationDescriptor(
        toSerializableFunction[Int, Int](_ + 2),
        basicDataUnitType[Int], basicDataUnitType[Int]
      )
    ), inputDataSet)
    addedValues.withName("Add 2")

    // Collect the result.
    val outputValues = addedValues.asInstanceOf[DataQuanta[Int]].collect()

    // Check the outcome.
    val expectedOutputValues = inputValues.map(_ + 2)
    Assert.assertArrayEquals(expectedOutputValues, outputValues.toArray)
  }

  @Test
  def testCustomOperatorShortCut(): Unit = {
    // Set up RheemContext.
    val rheem = new RheemContext()
    rheem.register(JavaPlatform.getInstance)
    rheem.register(SparkPlatform.getInstance)

    // Generate some test data.
    val inputValues = (for (i <- 1 to 10) yield i).toArray

    // Build and execute a Rheem plan.
    val outputValues = rheem
      .readCollection(inputValues).withName("Load input values")
      .customOperator[Int](new JavaMapOperator(
      dataSetType[Int],
      dataSetType[Int],
      new TransformationDescriptor(
        toSerializableFunction[Int, Int](_ + 2),
        basicDataUnitType[Int], basicDataUnitType[Int]
      )
    )).withName("Add 2")
      .collect()

    // Check the outcome.
    val expectedOutputValues = inputValues.map(_ + 2)
    Assert.assertArrayEquals(expectedOutputValues, outputValues.toArray)
  }

  @Test
  def testWordCount(): Unit = {
    // Set up RheemContext.
    val rheem = new RheemContext()
    rheem.register(JavaPlatform.getInstance)
    rheem.register(SparkPlatform.getInstance)

    // Generate some test data.
    val inputValues = Array("Big data is big.", "Is data big data?")

    // Build and execute a word count RheemPlan.
    val wordCounts = rheem
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
  def testWordCountOnSparkAndJava = {
    // Set up RheemContext.
    val rheem = new RheemContext()
    rheem.register(JavaPlatform.getInstance)
    rheem.register(SparkPlatform.getInstance)

    // Generate some test data.
    val inputValues = Array("Big data is big.", "Is data big data?")

    // Build and execute a word count RheemPlan.
    val wordCounts = rheem
      .readCollection(inputValues).withName("Load input values").withTargetPlatforms(JavaPlatform.getInstance)
      .flatMap(_.split("\\s+")).withName("Split words").withTargetPlatforms(JavaPlatform.getInstance)
      .map(_.replaceAll("\\W+", "").toLowerCase).withName("To lowercase").withTargetPlatforms(SparkPlatform.getInstance)
      .map((_, 1)).withName("Attach counter").withTargetPlatforms(SparkPlatform.getInstance)
      .reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2)).withName("Sum counters").withTargetPlatforms(SparkPlatform.getInstance)
      .collect().toSet

    val expectedWordCounts = Set(("big", 3), ("is", 2), ("data", 3))

    Assert.assertEquals(expectedWordCounts, wordCounts)
  }

  @Test
  def testDoWhile(): Unit = {
    // Set up RheemContext.
    val rheem = new RheemContext()
    rheem.register(JavaPlatform.getInstance)
    rheem.register(SparkPlatform.getInstance)

    // Generate some test data.
    val inputValues = Array(1, 2)

    // Build and execute a word count RheemPlan.

    val values = rheem
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
    val rheem = new RheemContext()
    rheem.register(JavaPlatform.getInstance)
    rheem.register(SparkPlatform.getInstance)

    // Generate some test data.
    val inputValues = Array(1, 2)

    // Build and execute a word count RheemPlan.

    val values = rheem
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
    val rheem = new RheemContext()
    rheem.register(JavaPlatform.getInstance)
    rheem.register(SparkPlatform.getInstance)

    // Generate some test data.
    val inputStrings = Array("Hello", "World", "Hi", "Mars")
    val selectors = Array('o', 'l')

    val selectorsDataSet = rheem.readCollection(selectors).withName("Load selectors")

    // Build and execute a word count RheemPlan.
    val values = rheem
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

  @Test
  def testGroupBy() = {
    // Set up RheemContext.
    val rheem = new RheemContext()
    rheem.register(JavaPlatform.getInstance)
    rheem.register(SparkPlatform.getInstance)

    val inputValues = Array(1, 2, 3, 4, 5, 7, 8, 9, 10)

    val result = rheem
      .readCollection(inputValues)
      .groupByKey(_ % 2)
      .map {
        group =>
          import scala.collection.JavaConversions._
          val buffer = group.toBuffer
          buffer.sortBy(identity)
          if (buffer.size % 2 == 0) (buffer(buffer.size / 2 - 1) + buffer(buffer.size / 2)) / 2
          else buffer(buffer.size / 2)
      }
      .collect()

    val expectedValues = Set(5, 6)
    Assert.assertEquals(expectedValues, result.toSet)
  }

  @Test
  def testGroup() = {
    // Set up RheemContext.
    val rheem = new RheemContext()
    rheem.register(JavaPlatform.getInstance)
    rheem.register(SparkPlatform.getInstance)

    val inputValues = Array(1, 2, 3, 4, 5, 7, 8, 9, 10)

    val result = rheem
      .readCollection(inputValues)
      .group()
      .map {
        group =>
          import scala.collection.JavaConversions._
          val buffer = group.toBuffer
          buffer.sortBy(int => int)
          if (buffer.size % 2 == 0) (buffer(buffer.size / 2) + buffer(buffer.size / 2 + 1)) / 2
          else buffer(buffer.size / 2)
      }
      .collect()

    val expectedValues = Set(5)
    Assert.assertEquals(expectedValues, result.toSet)
  }
}
