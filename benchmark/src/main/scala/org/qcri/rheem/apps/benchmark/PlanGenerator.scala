package org.qcri.rheem.apps.benchmark

import de.hpi.isg.profiledb.store.model.Experiment
import org.qcri.rheem.api.{DataQuanta, PlanBuilder}
import org.qcri.rheem.core.api.RheemContext
import org.qcri.rheem.core.plan.rheemplan.RheemPlan

/**
  * Utility to create random [[RheemPlan]]s.
  */
abstract class PlanGenerator {

  /**
    * Generate a [[RheemPlan]].
    *
    * @param rheemCtx that should be used to generate the [[RheemPlan]]
    * @return a function that upon invocation triggers the execution of the [[RheemPlan]]
    */
  def generatePlanTrigger(rheemCtx: RheemContext, experiment: Experiment): () => Unit

}

class PipelinePlanGenerator(numOperators: Int) extends PlanGenerator {

  require(numOperators >= 2, "At least 2 operators are required for a pipeline.")

  override def generatePlanTrigger(rheemCtx: RheemContext, experiment: Experiment): () => Unit = {
    val planBuilder = new PlanBuilder(rheemCtx, s"Generated pipeline ($numOperators OPs)")
      .withExperiment(experiment)
    val source = planBuilder.loadCollection(Seq(1))
    var edge: DataQuanta[Int] = source.withName("source")
    for (i <- 2 until numOperators) edge = edge.map(i => i).withName(s"intermediate ${i - 1}")
    () => edge.collect()
  }

}


class FanoutPlanGenerator(fanDegree: Int) extends PlanGenerator {

  require(fanDegree >= 1, "A fan degree must be >=1.")

  override def generatePlanTrigger(rheemCtx: RheemContext, experiment: Experiment): () => Unit = {
    val planBuilder = new PlanBuilder(rheemCtx, s"Generated fanout (degree $fanDegree)")
      .withExperiment(experiment)
    val source = planBuilder.loadCollection(Seq(1)).withName("source")
    var edge: DataQuanta[Int] = source.map(i => i).withName("intermediate 1")
    for (i <- 2 to fanDegree) edge = edge.map(i => i).withName(s"intermediate $i").withBroadcast(source, "source")
    () => edge.collect()
  }

}



class TreePlanGenerator(treeHeight: Int) extends PlanGenerator {

  require(treeHeight >= 1, "The tree height must be >=1.")


  override def generatePlanTrigger(rheemCtx: RheemContext, experiment: Experiment): () => Unit = {
    val planBuilder = new PlanBuilder(rheemCtx, s"Generated tree (height $treeHeight)")
      .withExperiment(experiment)
    val root = createTree(planBuilder, treeHeight)
    () => root.collect()
  }

  /**
    * Create [[DataQuanta]] that from the root of a tree-shaped [[RheemPlan]] with the given height.
    * @param planBuilder to create the leave operators, which are sources
    * @param height of the tree
    * @return the tree root [[DataQuanta]]
    */
  private def createTree(planBuilder: PlanBuilder, height: Int): DataQuanta[Int] =
    if (height == 1) planBuilder.loadCollection(Seq(1))
    else createTree(planBuilder, height - 1).union(createTree(planBuilder, height - 1))

}
