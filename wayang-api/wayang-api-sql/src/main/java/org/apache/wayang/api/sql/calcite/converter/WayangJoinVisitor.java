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

package org.apache.wayang.api.sql.calcite.converter;

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.wayang.api.sql.calcite.rel.WayangJoin;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.Operator;

public class WayangJoinVisitor extends WayangRelNodeVisitor<WayangJoin> {

    WayangJoinVisitor(WayangRelConverter wayangRelConverter) {
        super(wayangRelConverter);
    }

    @Override
    Operator visit(WayangJoin wayangRelNode) {
        Operator childOpLeft = wayangRelConverter.convert(wayangRelNode.getInput(0));
        Operator childOpRight = wayangRelConverter.convert(wayangRelNode.getInput(1));

        RexNode condition = ((Join) wayangRelNode).getCondition();

        if (!condition.isA(SqlKind.EQUALS)) {
            new UnsupportedOperationException("Only equality joins supported");
        }

        int offset = wayangRelNode.getInput(0).getRowType().getFieldCount();

        int leftKeyIndex = condition.accept(new KeyIndex(false, Child.LEFT));
        int rightKeyIndex = condition.accept(new KeyIndex(false, Child.RIGHT)) - offset;

        JoinOperator<Record, Record, Object> join = new JoinOperator<>(
                new TransformationDescriptor<>(new KeyExtractor(leftKeyIndex), Record.class, Object.class),
                new TransformationDescriptor<>(new KeyExtractor(rightKeyIndex), Record.class, Object.class)
        );

        //call connectTo on both operators (left and right)
        childOpLeft.connectTo(0, join, 0);
        childOpRight.connectTo(0, join, 1);

        MapOperator<Tuple2, Record> mapOperator = new MapOperator(
                new MapFunction(),
                Tuple2.class,
                Record.class
        );
        join.connectTo(0, mapOperator, 0);
        return mapOperator;
    }

    /**
     * Extracts key index from the call
     */
    private class KeyIndex extends RexVisitorImpl<Integer> {
        Child child;

        protected KeyIndex(boolean deep, Child child) {
            super(deep);
            this.child = child;
        }

        @Override
        public Integer visitCall(RexCall call) {
            RexNode operand = call.getOperands().get(child.ordinal());
            if (!(operand instanceof RexInputRef rexInputRef)) {
                throw new UnsupportedOperationException("Unsupported operation");
            }
            return rexInputRef.getIndex();
        }
    }

    /**
     * Extracts the key
     */
    private class KeyExtractor implements FunctionDescriptor.SerializableFunction<Record, Object> {
        int index;

        public KeyExtractor(int index) {
            this.index = index;
        }

        @Override
        public Object apply(Record record) {
            return record.getField(index);
        }
    }

    /**
     * Flattens the two Records from the Tuple2 into one Record
     * This is necessary for join to connect to project
     */
    private class MapFunction implements FunctionDescriptor.SerializableFunction<Tuple2<Record, Record>, Record> {
        public MapFunction() {
            super();
        }

        @Override
        public Record apply(Tuple2<Record, Record> tuple2) {
            Record r1 = tuple2.field0;
            Record r2 = tuple2.field1;

            return concatenatedValues(r1, r2);
        }

        /**
         * Concatenates the values from the two records
         * @param rec1
         * @param rec2
         * @return concatenated values
         */
        private Record concatenatedValues(Record rec1, Record rec2) {
            Object[] concatValues = new Object[rec1.size() + rec2.size()];

            for (int i = 0; i < rec1.size(); i++) {
                concatValues[i] = rec1.getField(i);
            }

            for (int i = rec1.size(); i < rec1.size() + rec2.size(); i++) {
                concatValues[i] = rec2.getField(i-rec1.size());
            }
            return new Record(concatValues);

        }
    }

    // Helpers
    private enum Child {
        LEFT, RIGHT
    }
}
