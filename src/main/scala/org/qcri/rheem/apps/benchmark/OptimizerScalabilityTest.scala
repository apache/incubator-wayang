package org.qcri.rheem.apps.benchmark

import org.qcri.rheem.apps.util.{ExperimentDescriptor, Parameters, ProfileDBHelper}
import org.qcri.rheem.core.api.{Configuration, RheemContext}

/**
  * This app provides scalability tests for Rheem's optimizer.
  */
object OptimizerScalabilityTest extends ExperimentDescriptor {

  /**
    * Creates a [[RheemContext]] that skips execution.
    */
  def createRheemContext(platformParameter: String) = {
    val config = new Configuration
    config.getProperties.set("rheem.core.debug.skipexecution", "true")
    val rheemContext = new RheemContext(config)
    Parameters.loadPlugins(platformParameter).foreach(rheemContext.register)
    rheemContext
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

    // Initialize the rheemContext.
    val rheemContext = createRheemContext(args(1))
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
    val planTrigger = planGenerator.generatePlanTrigger(rheemContext, experiment)
    planTrigger()

    // Store the experiment.
    ProfileDBHelper.store(experiment, rheemContext.getConfiguration)
  }
}
