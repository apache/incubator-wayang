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

import org.apache.wayang.commons.util.profiledb.model.Experiment
import org.apache.wayang.api.{DataQuanta, PlanBuilder}
import org.apache.wayang.core.api.WayangContext
import org.apache.wayang.core.plan.wayangplan.WayangPlan

/**
  * Utility to create random [[WayangPlan]]s.
  */
abstract class PlanGenerator {

  /**
    * Generate a [[WayangPlan]].
    *
    * @param wayangCtx that should be used to generate the [[WayangPlan]]
    * @return a function that upon invocation triggers the execution of the [[WayangPlan]]
    */
  def generatePlanTrigger(wayangCtx: WayangContext, experiment: Experiment): () => Unit

}

class PipelinePlanGenerator(numOperators: Int) extends PlanGenerator {

  require(numOperators >= 2, "At least 2 operators are required for a pipeline.")

  override def generatePlanTrigger(wayangCtx: WayangContext, experiment: Experiment): () => Unit = {
    val planBuilder = new PlanBuilder(wayangCtx, s"Generated pipeline ($numOperators OPs)")
      .withExperiment(experiment)
    val source = planBuilder.loadCollection(Seq(1))
    var edge: DataQuanta[Int] = source.withName("source")
    for (i <- 2 until numOperators) edge = edge.map(i => i).withName(s"intermediate ${i - 1}")
    () => edge.collect()
  }

}


class FanoutPlanGenerator(fanDegree: Int) extends PlanGenerator {

  require(fanDegree >= 1, "A fan degree must be >=1.")

  override def generatePlanTrigger(wayangCtx: WayangContext, experiment: Experiment): () => Unit = {
    val planBuilder = new PlanBuilder(wayangCtx, s"Generated fanout (degree $fanDegree)")
      .withExperiment(experiment)
    val source = planBuilder.loadCollection(Seq(1)).withName("source")
    var edge: DataQuanta[Int] = source.map(i => i).withName("intermediate 1")
    for (i <- 2 to fanDegree) edge = edge.map(i => i).withName(s"intermediate $i").withBroadcast(source, "source")
    () => edge.collect()
  }

}



class TreePlanGenerator(treeHeight: Int) extends PlanGenerator {

  require(treeHeight >= 1, "The tree height must be >=1.")


  override def generatePlanTrigger(wayangCtx: WayangContext, experiment: Experiment): () => Unit = {
    val planBuilder = new PlanBuilder(wayangCtx, s"Generated tree (height $treeHeight)")
      .withExperiment(experiment)
    val root = createTree(planBuilder, treeHeight)
    () => root.collect()
  }

  /**
    * Create [[DataQuanta]] that from the root of a tree-shaped [[WayangPlan]] with the given height.
    * @param planBuilder to create the leave operators, which are sources
    * @param height of the tree
    * @return the tree root [[DataQuanta]]
    */
  private def createTree(planBuilder: PlanBuilder, height: Int): DataQuanta[Int] =
    if (height == 1) planBuilder.loadCollection(Seq(1))
    else createTree(planBuilder, height - 1).union(createTree(planBuilder, height - 1))

}
