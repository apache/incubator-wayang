package org.qcri.rheem.apps.util

import org.qcri.rheem.core.api.Configuration
import org.qcri.rheem.core.platform.Platform
import org.qcri.rheem.core.plugin.Plugin
import org.qcri.rheem.graphchi.GraphChiPlatform
import org.qcri.rheem.java.JavaPlatform
import org.qcri.rheem.postgres.PostgresPlatform
import org.qcri.rheem.spark.platform.SparkPlatform
import org.qcri.rheem.sqlite3.Sqlite3Platform
import org.slf4j.LoggerFactory

/**
  * Utility to parse parameters of the apps.
  */
object Parameters {

  /**
    * Factories for [[Platform]]s
    */
  private val pluginFactories: Map[String, () => Plugin] = Map(
    "java" -> (() => JavaPlatform.getInstance),
    "spark" -> (() => SparkPlatform.getInstance),
    "graphchi" -> (() => GraphChiPlatform.getInstance),
    "postgres" -> (() => PostgresPlatform.getInstance),
    "sqlite3" -> (() => Sqlite3Platform.getInstance)
  )

  /**
    * Load a plugin.
    *
    * @param id            name of the plugin; append `+` to warm up the platform
    * @param configFactory creates a [[Configuration]] to warm up platforms
    * @return the loaded, and possibly warmed-up [[Plugin]]/[[Platform]]
    */
  def loadPlugin(id: String, configFactory: () => Configuration = () => new Configuration): Plugin = {
    val warmPlatform = """(\w+)\+""".r
    id match {
      case warmPlatform(name) => {
        val plugin = pluginFactories
          .getOrElse(name, throw new IllegalArgumentException(s"Unknown plugin: $name"))
          .apply
        plugin match {
          case platform: Platform => platform.warmUp(configFactory())
          case _ => LoggerFactory.getLogger(this.getClass).warn(s"Could not warm up $name.")
        }
        plugin
      }
      case name: String => pluginFactories
        .getOrElse(name, throw new IllegalArgumentException(s"Unknown platform: $name"))
        .apply
      case _ => throw new IllegalArgumentException(s"Illegal platform name")
    }
  }

  /**
    * Loads the specified [[Plugin]]s..
    *
    * @param platformIds   a comma-separated list of platform IDs
    * @param configFactory creates a [[Configuration]] to warm up platforms
    * @return the loaded, and possibly warmed-up [[Plugin]]s
    */
  def loadPlugins(platformIds: String, configFactory: () => Configuration): Seq[Plugin] =
    loadPlugins(platformIds.split(","), configFactory)

  /**
    * Loads the specified [[Plugin]]s.
    *
    * @param platformIds   platform IDs
    * @param configFactory creates a [[Configuration]] to warm up platforms
    * @return the loaded, and possibly warmed-up [[Plugin]]s
    */
  def loadPlugins(platformIds: Seq[String], configFactory: () => Configuration): Seq[Plugin] = {
    val configuration = configFactory()
    platformIds.map(loadPlugin(_, () => configuration))
  }

}
