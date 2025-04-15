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
 *
 */

package org.apache.wayang.api.sql.calcite.converter;

import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;

import org.apache.wayang.api.sql.calcite.converter.functions.ProjectMapFuncImpl;
import org.apache.wayang.api.sql.calcite.rel.WayangProject;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.plan.wayangplan.Operator;

import java.util.List;

public class WayangProjectVisitor extends WayangRelNodeVisitor<WayangProject> {
    WayangProjectVisitor(final WayangRelConverter wayangRelConverter) {
        super(wayangRelConverter);
    }

    @Override
    Operator visit(final WayangProject wayangRelNode) {
        final Operator childOp = wayangRelConverter.convert(wayangRelNode.getInput(0));

        /* Quick check */
        final List<RexNode> projects = ((Project) wayangRelNode).getProjects();

        // TODO: create a map with specific dataset type
        final MapOperator<Record, Record> projection = new MapOperator<>(
                new ProjectMapFuncImpl(projects),
                Record.class,
                Record.class);

        childOp.connectTo(0, projection, 0);

        return projection;
    }
}