package org.qcri.rheem.apps.util

import org.qcri.rheem.core.api.Configuration
import org.qcri.rheem.core.platform.Platform
import org.qcri.rheem.graphchi.GraphChiPlatform
import org.qcri.rheem.java.JavaPlatform
import org.qcri.rheem.postgres.PostgresPlatform
import org.qcri.rheem.spark.platform.SparkPlatform

/**
  * Utility to parse parameters of the apps.
  */
object Parameters {

  /**
    * Factories for [[Platform]]s
    */
  private val platformFactories = Map(
    "java" -> (() => JavaPlatform.getInstance),
    "spark" -> (() => SparkPlatform.getInstance),
    "graphchi" -> (() => GraphChiPlatform.getInstance),
    "postgres" -> (() => PostgresPlatform.getInstance)
  )

  /**
    * Load a platform.
    *
    * @param id            name of the platform; append `+` to warm up the platform
    * @param configFactory creates a [[Configuration]] to warm up platforms
    * @return the loaded, and possibly warmed-up [[Platform]]
    */
  def loadPlatform(id: String, configFactory: () => Configuration = () => new Configuration): Platform = {
    val warmPlatform = """(\w+)\+""".r
    id match {
      case warmPlatform(name) => {
        val platform = platformFactories
          .getOrElse(name, throw new IllegalArgumentException("Unknown platform: \"$name\""))
          .apply
        platform.warmUp(configFactory())
        platform
      }
      case name: String => platformFactories
        .getOrElse(name, throw new IllegalArgumentException("Unknown platform: \"$name\""))
        .apply
      case _ => throw new IllegalArgumentException(s"Illegal platform name")
    }
  }

  /**
    * Loads the specified platforms.
    *
    * @param platformIds   a comma-separated list of platform IDs
    * @param configFactory creates a [[Configuration]] to warm up platforms
    * @return the loaded, and possibly warmed-up [[Platform]]s
    */
  def loadPlatforms(platformIds: String, configFactory: () => Configuration = () => new Configuration): Seq[Platform] =
    loadPlatforms(platformIds.split(","), configFactory)

  /**
    * Loads the specified platforms.
    *
    * @param platformIds   platform IDs
    * @param configFactory creates a [[Configuration]] to warm up platforms
    * @return the loaded, and possibly warmed-up [[Platform]]s
    */
  def loadPlatforms(platformIds: Seq[String], configFactory: () => Configuration = () => new Configuration): Seq[Platform] = {
    val configuration = configFactory()
    platformIds.map(loadPlatform(_, () => configuration))
  }

}
