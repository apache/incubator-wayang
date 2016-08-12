package org.qcri.rheem.apps.util

import de.hpi.isg.profiledb.store.model.Experiment
import org.qcri.rheem.basic.RheemBasics
import org.qcri.rheem.core.plugin.{DynamicPlugin, Plugin}
import org.qcri.rheem.graphchi.GraphChi
import org.qcri.rheem.java.Java
import org.qcri.rheem.postgres.Postgres
import org.qcri.rheem.spark.Spark
import org.qcri.rheem.sqlite3.Sqlite3

/**
  * Utility to parse parameters of the apps.
  */
object Parameters {

  private val yamlId = """yaml\((.*)\)""".r

  val yamlPluginHel = "yaml(<YAML plugin URL>)"

  private val experiment = """exp\(([^,]*)((?:,[^,]*)*)\)""".r

  val experimentHelp = "exp(<ID>(,<tag>)*)"

  /**
    * Load a plugin.
    *
    * @param id name of the plugin
    * @return the loaded [[Plugin]]
    */
  def loadPlugin(id: String): Plugin = id match {
    case "basic-graph" => RheemBasics.graphPlugin
    case "java" => Java.basicPlugin
    case "java-graph" => Java.graphPlugin
    case "java-conversions" => Java.channelConversionPlugin
    case "spark" => Spark.basicPlugin
    case "spark-graph" => Spark.graphPlugin
    case "graphchi" => GraphChi.plugin
    case "postgres" => Postgres.plugin
    case "sqlite3" => Sqlite3.plugin
    case yamlId(url) => DynamicPlugin.loadYaml(url)
    case other => throw new IllegalArgumentException(s"Could not load platform '$other'.")
  }

  /**
    * Loads the specified [[Plugin]]s..
    *
    * @param platformIds a comma-separated list of platform IDs
    * @return the loaded [[Plugin]]s
    */
  def loadPlugins(platformIds: String): Seq[Plugin] = loadPlugins(platformIds.split(","))

  /**
    * Loads the specified [[Plugin]]s.
    *
    * @param platformIds platform IDs
    * @return the loaded [[Plugin]]s
    */
  def loadPlugins(platformIds: Seq[String]): Seq[Plugin] = platformIds.map(loadPlugin)

  /**
    * Create an [[Experiment]] for an experiment parameter and an [[ExperimentDescriptor]].
    *
    * @param experimentParameter  the parameter
    * @param experimentDescriptor the [[ExperimentDescriptor]]
    * @return the [[Experiment]]
    */
  def createExperiment(experimentParameter: String, experimentDescriptor: ExperimentDescriptor) =
  experimentParameter match {
    case experiment(id, tagList) => {
      val tags = tagList.split(',').filterNot(_.isEmpty)
      experimentDescriptor.createExperiment(id, tags: _*)
    }
    case other => throw new IllegalArgumentException(s"Could parse experiment descriptor '$other'.")
  }

}
