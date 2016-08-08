package org.qcri.rheem.apps.crocopr

import org.qcri.rheem.api.{DataQuanta, PlanBuilder}
import org.qcri.rheem.apps.util.{Parameters, StdOut}
import org.qcri.rheem.core.api.RheemContext
import org.qcri.rheem.core.api.exception.RheemException
import org.qcri.rheem.core.plugin.Plugin
import org.qcri.rheem.core.util.RheemCollections
import org.qcri.rheem.api.graph._

/**
  * Rheem implementation of the cross-community PageRank.
  */
class CrocoPR(plugins: Plugin*) {

  /**
    * Executes the cross-community PageRank on the given files.
    *
    * @param inputUrl1 URL to the first RDF N3 file
    * @param inputUrl2 URL to the second RDF N3 file
    * @return the page ranks
    */
  def apply(inputUrl1: String, inputUrl2: String, numIterations: Int) = {
    // Initialize.
    val rheemCtx = new RheemContext
    plugins.foreach(rheemCtx.register)
    implicit val planBuilder = new PlanBuilder(rheemCtx)

    // Read the input files.
    val links1 = readLinks(inputUrl1)
    val links2 = readLinks(inputUrl2)

    // Merge the links.
    val allLinks = links1
      .union(links2).withName("Union links")
      .distinct.withName("Distinct links")

    // Create vertex IDs.
    val vertexIds = allLinks
      .flatMap(link => Seq(link._1, link._2)).withName("Flatten vertices")
      .distinct.withName("Distinct vertices")
      .zipWithId.withName("Add vertex IDs")


    type VertexId = org.qcri.rheem.basic.data.Tuple2[Vertex, String]
    val edges = allLinks
      .join[VertexId, String](_._1, vertexIds, _.field1).withName("Join source vertex IDs")
      .map { linkAndVertexId =>
        (linkAndVertexId.field1.field0, linkAndVertexId.field0._2)
      }.withName("Set source vertex ID")
      .join[VertexId, String](_._2, vertexIds, _.field1).withName("Join target vertex IDs")
      .map(linkAndVertexId => new Edge(linkAndVertexId.field0._1, linkAndVertexId.field1.field0)).withName("Set target vertex ID")

    // Run the PageRank.
    val pageRanks = edges.pageRank(numIterations)

    // Make the page ranks readable.
    pageRanks
      .map(identity).withName("Hotfix")
      .join[VertexId, Long](_.field0, vertexIds, _.field0).withName("Join page ranks with vertex IDs")
      .map(joinTuple => (joinTuple.field1.field1, joinTuple.field0.field1)).withName("Make page ranks readable")
      .withUdfJarsOf(this.getClass)
      .collect(jobName = s"CrocoPR ($inputUrl1, $inputUrl2, $numIterations iterations)")

  }

  /**
    * Reads and parses an input file.
    *
    * @param inputUrl    URL to the file
    * @param planBuilder used to build to create Rheem operators
    * @return [[DataQuanta]] representing the parsed file
    */
  def readLinks(inputUrl: String)(implicit planBuilder: PlanBuilder): DataQuanta[(String, String)] = {
    val linkPattern =
      """<http://dbpedia.org/resource/([^>]+)>\s+<http://dbpedia.org/ontology/wikiPageWikiLink>\s+<http://dbpedia.org/resource/([^>]+)>\s+\.""".r

    planBuilder
      .readTextFile(inputUrl).withName(s"Load $inputUrl")
      .filter(!_.startsWith("#")).withName("Filter comments")
      .map {
        case linkPattern(source, target) => (source, target)
        case str => throw new RheemException(s"Cannot parse $str.")
      }.withName("Parse lines")
  }

}

/**
  * Companion object for [[CrocoPR]].
  */
object CrocoPR {

  def main(args: Array[String]) {
    // Parse parameters.
    if (args.isEmpty) {
      sys.error("Usage: <main class> <plugin>(,<plugin>)* <input URL1> <input URL2> <#iterations>")
      sys.exit(1)
    }
    val plugins = Parameters.loadPlugins(args(0))
    val inputUrl1 = args(1)
    val inputUrl2 = args(2)
    val numIterations = args(3).toInt

    // Prepare the PageRank.
    val pageRank = new CrocoPR(plugins: _*)

    // Run the PageRank.
    val pageRanks = pageRank(inputUrl1, inputUrl2, numIterations).toSeq.sortBy(-_._2)

    // Print the result.
    println(s"Calculated ${pageRanks.size} page ranks:")
    StdOut.printLimited(pageRanks, formatter = (pr: (String, Float)) => s"${pr._1} has a page rank of ${pr._2}")
  }

}


