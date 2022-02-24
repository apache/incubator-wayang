/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.wayang.api.sql.relation.node;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.List;

public class WayangProject extends Project implements WayangRel {
  public WayangProject(RelOptCluster cluster, RelTraitSet traits, List<RelHint> hints, RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
    super(cluster, traits, hints, input, projects, rowType);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    RelOptCost tmp = planner.getCostFactory().makeCost(10, 10, 10);
    System.out.println("heree "+ tmp.isInfinite() + "  "+ tmp.toString() );
    return tmp;
  }

  @Override
  public void implement(Implementor implementor) {
    System.out.println("project :D");
  }

  @Override
  public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
    return new WayangProject(input.getCluster(), traitSet, Collections.emptyList(), input, projects, rowType);
  }


}
