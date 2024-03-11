package org.apache.wayang.api.json

import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson
import org.apache.wayang.api._
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.java.Java
import org.apache.wayang.api.json.builder.JsonPlanBuilder
import org.apache.wayang.api.json.parserutil.ParseOperatorsFromJson.parseOperatorsFromFile
import org.apache.wayang.api.graph._

object Main {

  def main0(args : Array[String]): Unit = {
    // main1()
    // main2()
  }

  def main1() {
    println( "Hello World!" )

    val inputUrl = "file:///home/mike/in1.txt"

    // Get a plan builder.
    val wayangContext = new WayangContext(new Configuration)
      .withPlugin(Java.basicPlugin)
    val planBuilder = new PlanBuilder(wayangContext)
      .withJobName(s"WordCount ($inputUrl)")
      .withUdfJarsOf(this.getClass)

    val wordcounts = planBuilder
      // Read the text file.
      .loadCollection(List("123 234 345 345 123")).withName("Load collection")


      // Split each line by non-word characters.
      .flatMap(_.split("\\s+"), selectivity = 10).withName("Split words")

      // Filter empty tokens.
      .filter(_.nonEmpty, selectivity = 0.99).withName("Filter empty words")

      // Attach counter to each word.
      .map(word => (word.toLowerCase, 1)).withName("To lower case, add counter")

      // Sum up counters for every word.
      .reduceByKey(_._1, (c1, c2) => (c1._1, c1._2 + c2._2)).withName("Add counters")
      .withCardinalityEstimator((in: Long) => math.round(in * 0.01))

      // Execute the plan and collect the results.
      .collect()

    println(wordcounts)
  }

  def main2(): Unit = {

    val filename = "plan-c.json"
    val operators: List[OperatorFromJson] = parseOperatorsFromFile(filename).get
    println("PARSED")
    operators.foreach(x => println(x))
    println()

    new JsonPlanBuilder()
      .setOperators(operators)
      .execute()
    println("Written out.")
    println()

  }

}
