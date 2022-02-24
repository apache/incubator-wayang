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

package org.apache.wayang.api.sql.rule;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;

import java.util.Collections;
import java.util.List;
import org.apache.wayang.api.sql.relation.node.WayangFilter;
import org.apache.wayang.api.sql.relation.node.WayangProject;
import org.apache.wayang.api.sql.relation.node.WayangRel;
import org.apache.wayang.api.sql.relation.node.WayangValues;

public class WayangRules {
  public static final List<ConverterRule> ALL_WAYANG_OPT_RULES =
      ImmutableList.of(
          WayangFilterRule.INSTANCE,
          WayangProjectRule.INSTANCE,
          WayangValuesRule.INSTANCE
      )
      ;

  private static class WayangFilterRule extends ConverterRule {
    private static final WayangFilterRule INSTANCE = new WayangFilterRule();

    private WayangFilterRule() {
      super(LogicalFilter.class, Convention.NONE, WayangRel.CONVENTION, "WayangFilterRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalFilter filter = (LogicalFilter) rel;
      final RelTraitSet traitSet = filter.getTraitSet().replace(0, WayangRel.CONVENTION);
      return new WayangFilter(rel.getCluster(), traitSet,
          convert(filter.getInput(), WayangRel.CONVENTION), filter.getCondition());
    }
  }

  private static class WayangProjectRule extends ConverterRule {
    private static final WayangProjectRule INSTANCE = new WayangProjectRule();

    private WayangProjectRule() {
      super(LogicalProject.class, Convention.NONE, WayangRel.CONVENTION, "WayangProjectRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalProject project = (LogicalProject) rel;
      final RelTraitSet traitSet = project.getTraitSet().replace(0, WayangRel.CONVENTION);


      return new WayangProject(project.getCluster(), traitSet, Collections.emptyList(), convert(project, WayangRel.CONVENTION),
          project.getProjects(), project.getRowType());
    }
  }

  private static class WayangValuesRule extends ConverterRule {
    private static final WayangValuesRule INSTANCE = new WayangValuesRule();
    private WayangValuesRule() {
      super(LogicalValues.class, Convention.NONE, WayangRel.CONVENTION, "WayangValueRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
      final LogicalValues values = (LogicalValues) rel;
      final RelTraitSet traitSet = values.getTraitSet().replace(0, WayangRel.CONVENTION);
      return new WayangValues(values.getCluster(), values.getRowType(), values.getTuples(), traitSet);
    }
  }
}
