/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.api.sql.calcite.optimizer;

import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;
import org.apache.wayang.api.sql.calcite.rules.WayangRules;

import java.util.List;

public class WayangProgram implements Program {

    private final RuleSet rules;

    WayangProgram(RuleSet rules) {
        this.rules = rules;
    }

    @Override
    public RelNode run(RelOptPlanner relOptPlanner, RelNode relNode, RelTraitSet relTraitSet, List<RelOptMaterialization> list, List<RelOptLattice> list1) {

        final HepProgramBuilder builder = HepProgram.builder();
        for(RelOptRule rule : rules) {
            builder.addRuleInstance(rule);
        }
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);

        Program hep = Programs.of(builder.build(), false, null);
        RelNode run = hep.run(relOptPlanner, relNode, relTraitSet, list, list1);

        return run;
    }
}
