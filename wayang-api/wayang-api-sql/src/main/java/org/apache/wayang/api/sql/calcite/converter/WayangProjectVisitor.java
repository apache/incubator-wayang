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
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.util.Tuple;
import scala.Tuple1;

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
        for (RexNode rexNode : projects) {
            if (!(rexNode instanceof RexInputRef)) {
                throw new IllegalStateException("Generalized Projections not supported yet");
            }
        }

        //TODO: create a map with specific dataset type
        MapOperator<Tuple2<Record, Record>, Record> projection = new MapOperator(
                new JoinToProject(projects),
//                Tuple.class,
                Tuple2.class,
                Record.class);


        childOp.connectTo(0, projection, 0);

        return projection;
    }

    private class JoinToProject implements FunctionDescriptor.SerializableFunction<Tuple2<Record, Record>, Record> {
        private final int[] fields;

        public JoinToProject(int[] fields) {
            this.fields = fields;
        }

        private JoinToProject(List<RexNode> projects) {
            this(getProjectFields(projects));
        }

        public static Record concatenateRecords(Record record1, Record record2) {
            int totalLength1 = record1.size();
            int totalLength2 = record2.size();

            Object[] concatenatedValues = new Object[totalLength1+totalLength2];

            for (int i = 0; i < totalLength1; i++) {
                concatenatedValues[i] = record1.getField(i);
            }

            for (int i = totalLength1; i < totalLength2; i++) {
                concatenatedValues[i] = record2.getField(i);
            }

            return new Record(concatenatedValues);
        }


        @Override
        public Record apply(Tuple2 tuple) {
            List<Object> projectedRecord = new ArrayList<>();
//            Record rec = new Record(((Record) tuple.field0), ((Record) tuple.field1));
            Record rec = new Record(concatenateRecords(((Record) tuple.field0), ((Record) tuple.field1)));



            ArrayList<Integer> fields = new ArrayList<>();
            fields.add(0);
            fields.add(1);
            fields.add(12);


            for (int field : fields) {
//                System.out.println(((Record) tuple.field0).size());
//                System.out.println(((Record) tuple.field1).size());

                projectedRecord.add(rec.getField(field));
            }

//            for (int field : fields) {
////                System.out.println(((Record) tuple.field0).size());
////                System.out.println(((Record) tuple.field1).size());
//
//                projectedRecord.add(rec.getField(field));
//            }

            return new Record(projectedRecord.toArray(new Object[0]));
        }
    }

    private class MapFunctionImpl implements
            FunctionDescriptor.SerializableFunction<Record, Record> {

        private final int[] fields;

        private MapFunctionImpl(int[] fields) {
            this.fields = fields;
        }

        private MapFunctionImpl(List<RexNode> projects) {
            this(getProjectFields(projects));
        }

        @Override
        public Record apply(Record record) {

            List<Object> projectedRecord = new ArrayList<>();
            for (int field : fields) {
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
