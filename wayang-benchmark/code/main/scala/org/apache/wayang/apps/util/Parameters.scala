/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.apps.util

import org.apache.wayang.basic.WayangBasics
import org.apache.wayang.commons.util.profiledb.model.Experiment
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval
import org.apache.wayang.core.plugin.{DynamicPlugin, Plugin}
//import org.apache.wayang.graphchi.GraphChi
import org.apache.wayang.java.Java
import org.apache.wayang.postgres.Postgres
import org.apache.wayang.spark.Spark
import org.apache.wayang.sqlite3.Sqlite3

/**
  * Utility to parse parameters of the apps.
  */
object Parameters {

  private val yamlId = """yaml\((.*)\)""".r

  val yamlPluginHel = "yaml(<YAML plugin URL>)"

  private val intPattern = """[+-]?\d+""".r

  private val longPattern = """[+-]?\d+L""".r

  private val doublePattern = """[+-]?\d+\.\d*""".r

  private val booleanPattern = """(?:true)|(?:false)""".r

  private val probabilisticDoubleIntervalPattern = """(\d+)\.\.(\d+)(~\d+.\d+)?""".r

  private val experiment =
    """exp\(([^,;]+)(?:;tags=([^,;]+(?:,[^,;]+)*))?(?:;conf=([^,;:]+:[^,;:]+(?:,[^,;:]+:[^,;:]+)*))?\)""".r

  val experimentHelp = "exp(<ID>[,tags=<tag>,...][,conf=<key>:<value>,...])"

  /**
    * Load a plugin.
    *
    * @param id name of the plugin
    * @return the loaded [[Plugin]]
    */
  def loadPlugin(id: String): Plugin = id match {
    case "basic-graph" => WayangBasics.graphPlugin
    case "java" => Java.basicPlugin
    case "java-graph" => Java.graphPlugin
    case "java-conversions" => Java.channelConversionPlugin
    case "spark" => Spark.basicPlugin
    case "spark-graph" => Spark.graphPlugin
    case "spark-conversions" => Spark.conversionPlugin
//    case "graphchi" => GraphChi.plugin
    case "postgres" => Postgres.plugin
    case "postgres-conversions" => Postgres.conversionPlugin
    case "sqlite3" => Sqlite3.plugin
    case "sqlite3-conversions" => Sqlite3.conversionPlugin
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
    case experiment(id, tagList, confList) =>
      val tags = tagList match {
        case str: String => str.split(',').filterNot(_.isEmpty)
        case _ => Array[String]()
      }
      val experiment = experimentDescriptor.createExperiment(id, tags: _*)
      confList match {
        case str: String => str.split(',').map(_.split(':')).foreach { pair =>
          experiment.getSubject.addConfiguration(pair(0), parseAny(pair(1)))
        }
        case _ =>
      }
      experiment
    case other => throw new IllegalArgumentException(s"Could parse experiment descriptor '$other'.")
  }

  /**
    * Parses a given [[String]] into a specific basic type.
    *
    * @param str the [[String]]
    * @return the parsed value
    */
  def parseAny(str: String): AnyRef = {
    str match {
      case "null" => null
      case intPattern() => java.lang.Integer.valueOf(str)
      case longPattern() => java.lang.Long.valueOf(str.take(str.length - 1))
      case doublePattern() => java.lang.Double.valueOf(str)
      case booleanPattern() => java.lang.Boolean.valueOf(str)
      case probabilisticDoubleIntervalPattern(lower, upper, conf) =>
        new ProbabilisticDoubleInterval(lower.toDouble, upper.toDouble, if (conf == null) 1d else conf.substring(1).toDouble)
      case other: String => other
    }
  }

}
