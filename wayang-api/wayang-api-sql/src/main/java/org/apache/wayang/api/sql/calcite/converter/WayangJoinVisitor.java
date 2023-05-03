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
            throw new UnsupportedOperationException("Only equality joins supported");
        }

        //offset of the index in the right child
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

        // Join returns Tuple2 - map to a Record
        MapOperator<Tuple2, Record> mapOperator = new MapOperator(
                new MapFunctionImpl(),
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
        final Child child;

        protected KeyIndex(boolean deep, Child child) {
            super(deep);
            this.child = child;
        }

        @Override
        public Integer visitCall(RexCall call) {
            RexNode operand = call.getOperands().get(child.ordinal());
            if (!(operand instanceof RexInputRef)) {
                throw new UnsupportedOperationException("Unsupported operation");
            }
            RexInputRef rexInputRef = (RexInputRef) operand;
            return rexInputRef.getIndex();
        }
    }

    /**
     * Extracts the key
     */
    private class KeyExtractor implements FunctionDescriptor.SerializableFunction<Record, Object> {
        private final int index;

        public KeyExtractor(int index) {
            this.index = index;
        }

        public Object apply(final Record record) {
            return record.getField(index);
        }
    }

    /**
     * Flattens Tuple2<Record, Record> to Record
     */
    private class MapFunctionImpl implements FunctionDescriptor.SerializableFunction<Tuple2<Record, Record>, Record> {
        public MapFunctionImpl() {
            super();
        }

        @Override
        public Record apply(final Tuple2<Record, Record> tuple2) {
            int length1 = tuple2.getField0().size();
            int length2 = tuple2.getField1().size();

            int totalLength = length1 + length2;

            Object[] fields = new Object[totalLength];

            for (int i = 0; i < length1; i++) {
                fields[i] = tuple2.getField0().getField(i);
            }
            for (int j = length1; j < totalLength; j++) {
                fields[j] = tuple2.getField1().getField(j - length1);
            }
            return new Record(fields);

        }
    }

    // Helpers
    private enum Child {
        LEFT, RIGHT
    }
}
