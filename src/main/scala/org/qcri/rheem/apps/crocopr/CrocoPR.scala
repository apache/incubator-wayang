package org.qcri.rheem.apps.crocopr

import org.qcri.rheem.api.{DataQuanta, PlanBuilder}
import org.qcri.rheem.apps.util.Parameters
import org.qcri.rheem.core.api.RheemContext
import org.qcri.rheem.core.api.exception.RheemException
import org.qcri.rheem.core.plugin.Plugin
import org.qcri.rheem.core.util.RheemCollections

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


    type VertexId = org.qcri.rheem.basic.data.Tuple2[Long, String]
    val edges = allLinks
      .join[VertexId, String](_._1, vertexIds, _.field1).withName("Join source vertex IDs")
      .map { linkAndVertexId =>
        (linkAndVertexId.field1.field0, linkAndVertexId.field0._2)
      }.withName("Set source vertex ID")
      .join[VertexId, String](_._2, vertexIds, _.field1).withName("Join target vertex IDs")
      .map(linkAndVertexId => (linkAndVertexId.field0._1, linkAndVertexId.field1.field0)).withName("Set target vertex ID")

    // Count the pages to come up with the initial ranks.
    val numPages = vertexIds
      .count.withName("Count pages")

    // Create adjacency lists from the links.
    import scala.collection.JavaConversions._
    val adjacencyList = edges
      .groupByKey(_._1).withName("Group edges")
      .map(edgeGroup => (RheemCollections.getAny(edgeGroup)._1, edgeGroup.map(_._2))).withName("Create adjacency")

    // Run the PageRank.
    type Adjacency = (Long, Iterable[Long])
    val pageRanks = adjacencyList
      .mapJava[(Long, Double)](new CreateInitialPageRanks("numPages"))
      .withBroadcast(numPages, "numPages").withName("Create initial page ranks")
      .repeat(numIterations, { pageRanks =>
        pageRanks
          .join[Adjacency, Long](_._1, adjacencyList, _._1).withName("Join page ranks and adjacency list")
          .flatMap { joinTuple =>
            val adjacent = joinTuple.field1
            val sourceRank = joinTuple.field0._2

            val newRank = sourceRank / adjacent._2.size
            adjacent._2.map(targetVertex => (targetVertex, newRank))
          }.withName("Send partial ranks")
          .reduceByKey(_._1, (act1, act2) => (act1._1, act1._2 + act2._2)).withName("Add partial ranks")
      })

    // Make the page ranks readable.
    pageRanks
      .map(identity).withName("Hotfix")
      .join[VertexId, Long](_._1, vertexIds, _.field0).withName("Join page ranks with vertex IDs")
      .map(joinTuple => (joinTuple.field1.field1, joinTuple.field0._2)).withName("Make page ranks readable")
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
    pageRanks.foreach(pr => println(s"${pr._1} has a page rank of ${pr._2}"))
  }

}


