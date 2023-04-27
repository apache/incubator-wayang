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

        Operator leftChildOperator = wayangRelConverter.convert(wayangRelNode.getInput(0));
        Operator rightChildOperator = wayangRelConverter.convert(wayangRelNode.getInput(1));

        // Get the join condition. Only supports equality join
        RexNode condition = wayangRelNode.getCondition();
        if(!condition.isA(SqlKind.EQUALS)) {
            new UnsupportedOperationException("Only equality joins supported");
        }

        //offset of the index in the right child
        int offset = wayangRelNode.getInput(0).getRowType().getFieldCount();

        // get the offset of keys in left and right records
        int leftKeyIndex = condition.accept(new keyIndex(false, Child.LEFT));
        int rightKeyIndex = condition.accept(new keyIndex(false, Child.RIGHT)) - offset;


        JoinOperator<Record, Record, Object>  joinOperator = new JoinOperator<>(
                new TransformationDescriptor<>(new KeyUdf(leftKeyIndex), Record.class, Object.class),
                new TransformationDescriptor<>(new KeyUdf(rightKeyIndex), Record.class, Object.class)
        );


        leftChildOperator.connectTo(0,joinOperator,0);
        rightChildOperator.connectTo(0,joinOperator,1);

        // The join outputs a Tuple2<Record, Record>, lets translate that to a Record
        MapOperator<Tuple2, Record> mapOperator = new MapOperator(
                new MapFunctionImpl(),
                Tuple2.class,
                Record.class);
        joinOperator.connectTo(0,mapOperator,0);
        return mapOperator;

    }

    private class keyIndex extends RexVisitorImpl<Integer> {

        Child child;
        protected keyIndex(boolean deep, Child child) {
            super(deep);
            this.child = child;
        }

        @Override
        public Integer visitCall(RexCall call) {
            RexNode operand = call.getOperands().get(child.ordinal());
            if(!(operand instanceof RexInputRef)) {
                new UnsupportedOperationException("unsupported operation");
            }
            RexInputRef rexInputRef = (RexInputRef)operand;
            return rexInputRef.getIndex();
        }
    }

    private class KeyUdf implements FunctionDescriptor.SerializableFunction<Record, Object> {

        int index;
        public KeyUdf(int index) {
        this.index = index;
        }

        @Override
        public Object apply(Record record) {
            return record.getField(index);
        }
    }

   private class MapFunctionImpl implements
            FunctionDescriptor.SerializableFunction<Tuple2<Record,Record>, Record> {
        public MapFunctionImpl() {
            super();
        }

        @Override
        public Record apply(Tuple2<Record, Record> tuple2) {
            Record r1 = tuple2.getField0();
            Record r2 = tuple2.getField1();

            int totalSize = r1.size()+ r2.size();

            Object[] objects = new Object[totalSize];
            int i = 0;
            for(;i < r1.size(); i++) {
                objects[i] = r1.getField(i);
            }
            for(int j = 0 ; j < r2.size(); j++, i++) {
                objects[i] = r2.getField(j);
            }
            return new Record(objects);
        }
    }

    // Helpers
    private enum Child {
        LEFT, RIGHT
    }
}
