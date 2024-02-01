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

package org.apache.wayang.apps.benchmark

import org.apache.wayang.apps.util.{ExperimentDescriptor, Parameters, ProfileDBHelper}
import org.apache.wayang.apps.util.ProfileDBHelper
import org.apache.wayang.core.api.{Configuration, WayangContext}

/**
  * This app provides scalability tests for Wayang's optimizer.
  */
object OptimizerScalabilityTest extends ExperimentDescriptor {

  /**
    * Creates a [[WayangContext]] that skips execution.
    */
  def createWayangContext(platformParameter: String) = {
    val config = new Configuration
    config.getProperties.set("wayang.core.debug.skipexecution", "true")
    val wayangContext = new WayangContext(config)
    Parameters.loadPlugins(platformParameter).foreach(wayangContext.register)
    wayangContext
  }

  override def version = "0.1.0"

  /**
    * Main method to run this app.
    */
  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println("Usage: <experiment descriptor> <plugins> <plan type> <plan type args>...")
      println("Plan types:")
      println(" pipeline <pipeline length>")
      println(" fanout <fanout degree>")
      println(" tree <tree height>")
    }

    // Create the experiment.
    val experiment = Parameters.createExperiment(args(0), this)

    // Initialize the wayangContext.
    val wayangContext = createWayangContext(args(1))
    experiment.getSubject.addConfiguration("plugins", args(1))

    // Create the planGenerator.
    val planType = args(2)
    experiment.getSubject.addConfiguration("planType", planType)
    val planGenerator: PlanGenerator = planType match {
      case "pipeline" => new PipelinePlanGenerator(args(3).toInt)
      case "fanout" => new FanoutPlanGenerator(args(3).toInt)
      case "tree" => new TreePlanGenerator(args(3).toInt)
      case _ => sys.error(s"Unknown plan type: $planType")
    }

    // Generate and execute the plan.
    val planTrigger = planGenerator.generatePlanTrigger(wayangContext, experiment)
    planTrigger()

    // Store the experiment.
    ProfileDBHelper.store(experiment, wayangContext.getConfiguration)
  }
}
