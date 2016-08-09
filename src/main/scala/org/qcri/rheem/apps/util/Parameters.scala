package org.qcri.rheem.apps.util

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

}
