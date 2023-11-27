package org.apache.wayang.api.serialization

import org.apache.wayang.api.{PlanBuilder, createPlanBuilder}
import org.apache.wayang.core.api.WayangContext
import org.apache.wayang.java.Java
import org.junit.Test


class OperatorSerializationTests extends SerializationTestBase {

  @Test
  def testReadMapCollect(): Unit = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    // Generate some test data.
    val inputValues = (for (i <- 1 to 10) yield i).toArray

    // Build and execute a Wayang plan.
    val dq = wayang
      .loadCollection(inputValues).withName("Load input values")
      .map(_ + 2).withName("Add 2")

    dq.operator.getTargetPlatforms

    // Check the outcome.
    val expectedOutputValues = inputValues.map(_ + 2).map(_.toString).toList
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedOutputValues)
  }

  @Test
  def testFilterDistinctCount(): Unit = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    // Generate some test data.
    val inputValues = List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8)

    // Build and execute a Wayang plan.
    val dq = wayang
      .loadCollection(inputValues)
      .filter(n => n >= 4)
      .distinct
      .count

    // Check the outcome.
    val expected = List("5")
    serializeDeserializeExecuteAssert(dq.operator, wayang, expected)
  }

  @Test
  def testReduce(): Unit = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    // Generate some test data.
    val inputValues = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    // Build and execute a Wayang plan.
    val dq = wayang
      .loadCollection(inputValues)
      .reduce((a, b) => a + b)

    // Check the outcome.
    val expected = List("55")
    serializeDeserializeExecuteAssert(dq.operator, wayang, expected)
  }

  @Test
  def testWordCount(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues = Array("Big data is big.", "Is data big data?")

    val dq = wayang
      .loadCollection(inputValues).withName("Load input values")
      .flatMap(_.split("\\s+")).withName("Split words")
      .map(_.replaceAll("\\W+", "").toLowerCase).withName("To lowercase")
      .map((_, 1)).withName("Attach counter")
      .reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2)).withName("Sum counters")

    val expectedWordCounts = List(("big", 3), ("data", 3), ("is", 2)).map(_.toString())
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedWordCounts)
  }

  @Test
  def testGroupBy(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues = Array(1, 2, 3, 4, 5, 7, 8, 9, 10)

    val dq = wayang
      .loadCollection(inputValues)
      .groupByKey(_ % 2).withName("group odd and even")
      .map {
        group =>
          import scala.collection.JavaConversions._
          val buffer = group.toBuffer
          buffer.sortBy(identity)
          if (buffer.size % 2 == 0) (buffer(buffer.size / 2 - 1) + buffer(buffer.size / 2)) / 2
          else buffer(buffer.size / 2)
      }.withName("median")

    val expectedOutputValues = List("6", "5")
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedOutputValues)
  }

  @Test
  def testSort(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues1 = Array(3, 4, 5, 2, 1)

    val builder = new PlanBuilder(wayang)
    val dataQuanta1 = builder.loadCollection(inputValues1)
    val dq = dataQuanta1
      .sort(r => r)

    val expectedValues = List(1, 2, 3, 4, 5).map(_.toString)
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedValues)
  }

  @Test
  def testMapPartitions(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin())

    val dq = wayang
      .loadCollection(Seq(0, 1, 2, 3, 4, 6, 8))
      .mapPartitions { ints =>
        var (numOdds, numEvens) = (0, 0)
        ints.foreach(i => if ((i & 1) == 0) numEvens += 1 else numOdds += 1)
        Seq(("odd", numOdds), ("even", numEvens))
      }
      .reduceByKey(_._1, { case ((kind1, count1), (kind2, count2)) => (kind1, count1 + count2) })

    val expectedValues = List(("even", 5), ("odd", 2)).map(_.toString())
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedValues)
  }

  @Test
  def testZipWithId(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues = for (i <- 0 until 100; j <- 0 until 42) yield i

    val dq = wayang
      .loadCollection(inputValues)
      .zipWithId
      .groupByKey(_.field1)
      .map { group =>
        import scala.collection.JavaConversions._
        (group.map(_.field0).toSet.size, 1)
      }
      .reduceByKey(_._1, (t1, t2) => (t1._1, t1._2 + t2._2))

    val expectedValues = List((42, 100)).map(_.toString())
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedValues)
  }

  @Test
  def testJoin(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues1 = Array(("Water", 0), ("Tonic", 5), ("Juice", 10))
    val inputValues2 = Array(("Apple juice", "Juice"), ("Tap water", "Water"), ("Orange juice", "Juice"))

    val builder = new PlanBuilder(wayang)
    val dataQuanta1 = builder.loadCollection(inputValues1)
    val dataQuanta2 = builder.loadCollection(inputValues2)
    val dq = dataQuanta1
      .join[(String, String), String](_._1, dataQuanta2, _._2)
      .map(joinTuple => (joinTuple.field1._1, joinTuple.field0._2))

    val expectedValues = List(("Apple juice", 10), ("Tap water", 0), ("Orange juice", 10))
      .map(s => s.toString())
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedValues)
  }

  @Test
  def testJoin2(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues1 = Array(1, 3, 2, 4, 5)
    val inputValues2 = Array(3, 5)

    val builder = new PlanBuilder(wayang)
    val dataQuanta1 = builder.loadCollection(inputValues1)
    val dataQuanta2 = builder.loadCollection(inputValues2)
    val dq = dataQuanta1
      .join[Int, Int](n => n, dataQuanta2, n => n)
      .map(joinTuple => joinTuple.field0)

    val expectedValues = List("3", "5")
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedValues)
  }

//  @Test
  def testCoGroup(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues1 = Array(("Water", 0), ("Cola", 5), ("Juice", 10))
    val inputValues2 = Array(("Apple juice", "Juice"), ("Tap water", "Water"), ("Orange juice", "Juice"))

    val builder = new PlanBuilder(wayang)
    val dataQuanta1 = builder.loadCollection(inputValues1)
    val dataQuanta2 = builder.loadCollection(inputValues2)
    val dq = dataQuanta1
      .coGroup[(String, String), String](_._1, dataQuanta2, _._2)

    val expectedValues = List(
      (List(("Water", 0)), List(("Tap water", "Water"))),
      (List(("Cola", 5)), List()),
      (List(("Juice", 10)), List(("Apple juice", "Juice"), ("Orange juice", "Juice")))
    )
      .map(_.toString)
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedValues)
  }


  @Test
  def testUnion(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues1 = Array(1, 2, 3, 4)
    val inputValues2 = Array(0, 1, 3, 5)

    val builder = new PlanBuilder(wayang)
    val dataQuanta1 = builder.loadCollection(inputValues1)
    val dataQuanta2 = builder.loadCollection(inputValues2)
    val dq = dataQuanta1.union(dataQuanta2)

    val unionExpectedValues = List(1, 2, 3, 4, 0, 1, 3, 5).map(_.toString)
    serializeDeserializeExecuteAssert(dq.operator, wayang, unionExpectedValues)
  }

  @Test
  def testIntersect(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues1 = Array(1, 2, 3, 4, 5, 7, 8, 9, 10)
    val inputValues2 = Array(9, 0, 2, 3, 3, 4, 5, 7, 8, 11)

    val builder = new PlanBuilder(wayang)
    val dataQuanta1 = builder.loadCollection(inputValues1)
    val dataQuanta2 = builder.loadCollection(inputValues2)
    val dq = dataQuanta1.intersect(dataQuanta2)

    val intersectExpectedValues = List(2, 3, 4, 5, 7, 8, 9).map(_.toString)
    serializeDeserializeExecuteAssert(dq.operator, wayang, intersectExpectedValues)
  }

  @Test
  def testRepeat(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues = Array(1, 2)

    val dq = wayang
      .loadCollection(inputValues)
      .repeat(3,
        _.reduce(_ * _)
          .flatMap(v => Seq(v, v + 1))
      )

    // initial: 1,2 -> 1st: 2,3 -> 2nd: 6,7 => 3rd: 42,43
    val expectedValues = List("42", "43")
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedValues)
  }

  @Test
  def testRepeat2(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues = Array(1, 2, 3, 4, 5)

    val dq = wayang
      .loadCollection(inputValues)
      .repeat(10, _.map(_ + 1))

    val expectedValues = List("11", "12", "13", "14", "15")
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedValues)
  }

  @Test
  def testDoWhile(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues = Array(1, 2)

    val dq = wayang
      .loadCollection(inputValues)
      .doWhile[Int](vals => vals.max > 100, {
        start =>
          val sum = start.reduce(_ + _).withName("Sum")
          (start.union(sum), sum)
      })

    val expectedValues = List(1, 2, 3, 6, 12, 24, 48, 96, 192).map(_.toString)
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedValues)
  }

  @Test
  def testSample(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues = for (i <- 0 until 100) yield i

    val dq = wayang
      .loadCollection(inputValues)
      .sample(10)

    val tempFilenameOut = serializeDeserializeExecute(dq.operator, wayang)
    SerializationTestBase.assertOutputFileLineCount(tempFilenameOut, 10)
  }

}
