package org.apache.wayang.api.serialization

import org.apache.wayang.api.{PlanBuilder, createPlanBuilder}
import org.apache.wayang.core.api.WayangContext
import org.apache.wayang.java.Java
import org.junit.Test


class OperatorSerializationTests extends SerializationTestBase{

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

    // Check the outcome.
    val expectedOutputValues = inputValues.map(_ + 2).map(_.toString).toList
    serializeThenDeserializeThenAssertOutput(dq.operator, wayang, expectedOutputValues)
  }

  @Test
  def testWordCount(): Unit = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    // Generate some test data.
    val inputValues = Array("Big data is big.", "Is data big data?")

    // Build and execute a word count WayangPlan.
    val dq = wayang
      .loadCollection(inputValues).withName("Load input values")
      .flatMap(_.split("\\s+")).withName("Split words")
      .map(_.replaceAll("\\W+", "").toLowerCase).withName("To lowercase")
      .map((_, 1)).withName("Attach counter")
      .reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2)).withName("Sum counters")

    val expectedWordCounts = List(("big", 3), ("data", 3), ("is", 2)).map(_.toString())
    serializeThenDeserializeThenAssertOutput(dq.operator, wayang, expectedWordCounts)
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
    serializeThenDeserializeThenAssertOutput(dq.operator, wayang, expectedOutputValues)
  }

  @Test
  def testSort(): Unit = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues1 = Array(3, 4, 5, 2, 1)

    val builder = new PlanBuilder(wayang)
    val dataQuanta1 = builder.loadCollection(inputValues1)
    val dq = dataQuanta1
      .sort(r => r)

    val expectedValues = List(1, 2, 3, 4, 5).map(_.toString)
    serializeThenDeserializeThenAssertOutput(dq.operator, wayang, expectedValues)
  }

  @Test
  def testMapPartitions(): Unit = {
    // Set up WayangContext.
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
    serializeThenDeserializeThenAssertOutput(dq.operator, wayang, expectedValues)
  }

  @Test
  def testZipWithId(): Unit = {
    // Set up WayangContext.
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
    serializeThenDeserializeThenAssertOutput(dq.operator, wayang, expectedValues)
  }

}
