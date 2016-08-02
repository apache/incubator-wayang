package org.qcri.rheem.apps.util

import org.qcri.rheem.core.plugin.{DynamicPlugin, Plugin}
import org.qcri.rheem.graphchi.GraphChiPlatform
import org.qcri.rheem.java.JavaPlatform
import org.qcri.rheem.postgres.PostgresPlatform
import org.qcri.rheem.spark.platform.SparkPlatform
import org.qcri.rheem.sqlite3.Sqlite3Platform

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
    case "java" => JavaPlatform.getInstance
    case "spark" => SparkPlatform.getInstance
    case "graphchi" => GraphChiPlatform.getInstance
    case "postgres" => PostgresPlatform.getInstance
    case "sqlite3" => Sqlite3Platform.getInstance
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
