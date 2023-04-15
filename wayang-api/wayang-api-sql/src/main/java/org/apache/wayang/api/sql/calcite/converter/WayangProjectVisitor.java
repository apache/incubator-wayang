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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.wayang.api.sql.calcite.rel.WayangProject;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.plan.wayangplan.Operator;

import java.util.ArrayList;
import java.util.List;

public class WayangProjectVisitor extends WayangRelNodeVisitor<WayangProject> {
    WayangProjectVisitor(WayangRelConverter wayangRelConverter) {
        super(wayangRelConverter);
    }

    @Override
    Operator visit(WayangProject wayangRelNode) {

        Operator childOp = wayangRelConverter.convert(wayangRelNode.getInput(0));

        /* Quick check */
        List<RexNode> projects = ((Project) wayangRelNode).getProjects();
        for(RexNode rexNode : projects) {
            if (!(rexNode instanceof RexInputRef)) {
                throw new IllegalStateException("Generalized Projections not supported yet");
            }
        }

        //TODO: create a map with specific dataset type
        MapOperator<Record, Record> projection = new MapOperator(
                new MapFunctionImpl(projects),
                Record.class,
                Record.class);

        childOp.connectTo(0, projection, 0);

        return projection;
    }


    private class MapFunctionImpl implements
            FunctionDescriptor.SerializableFunction<Record, Record> {

        private final int[] fields;

        private MapFunctionImpl(int[] fields) {
            this. fields = fields;
        }

        private MapFunctionImpl(List<RexNode> projects) {
            this(getProjectFields(projects));
        }

        @Override
        public Record apply(Record record) {

            List<Object> projectedRecord = new ArrayList<>();
            for(int field : fields) {
                projectedRecord.add(record.getField(field));
            }

            return new Record(projectedRecord.toArray(new Object[0]));
        }
    }

    private static int[] getProjectFields(List<RexNode> projects) {
        final int[] fields = new int[projects.size()];
        for (int i = 0; i < projects.size(); i++) {
            final RexNode exp = projects.get(i);
            if (exp instanceof RexInputRef) {
                fields[i] = ((RexInputRef) exp).getIndex();
            } else {
                return null; // not a simple projection
            }
        }
        return fields;
    }
}
