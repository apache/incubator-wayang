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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Set;
import org.checkerframework.checker.nullness.qual.Nullable;

public class WayangRelFactories {

  public static final Context ALL_RHEEM_REL_FACTORIES =
      Contexts.of(
          WayangFilterFactory.INSTANCE,
          WayangValuesFactory.INSTANCE,
          WayangProjectFactory.INSTANCE
      );

  private WayangRelFactories() {
  }


  public static class WayangFilterFactory implements RelFactories.FilterFactory {

    public static final WayangFilterFactory INSTANCE = new WayangFilterFactory();

    @Override
    public RelNode createFilter(RelNode input, RexNode condition, Set<CorrelationId> variablesSet) {
      return new WayangFilter(input.getCluster(), input.getTraitSet().replace(WayangRel.CONVENTION), input, condition);
    }
  }

  public static class WayangValuesFactory implements RelFactories.ValuesFactory {

    public static final WayangValuesFactory INSTANCE = new WayangValuesFactory();

    @Override
    public RelNode createValues(RelOptCluster cluster, RelDataType rowType, List<ImmutableList<RexLiteral>> tuples) {
      return new WayangValues(cluster, rowType, (ImmutableList<ImmutableList<RexLiteral>>) tuples, cluster.traitSet());
    }
  }

  public static class WayangProjectFactory implements RelFactories.ProjectFactory {
    public static final WayangProjectFactory INSTANCE = new WayangProjectFactory();

    @Override
    public RelNode createProject(
        RelNode input,
        List<RelHint> hints,
        List<? extends RexNode> childExprs,
        @Nullable List<? extends String> fieldNames) {
      return new WayangProject(input.getCluster(), input.getTraitSet(), hints, input, childExprs, input.getRowType());
    }
  }
}
