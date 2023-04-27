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
import org.apache.wayang.api.sql.calcite.rel.WayangJoin;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;
import org.apache.wayang.core.util.Tuple;

import java.security.Key;

public class WayangJoinVisitor extends WayangRelNodeVisitor<WayangJoin> {

    WayangJoinVisitor(WayangRelConverter wayangRelConverter) {
        super(wayangRelConverter);
    }

    @Override
    Operator visit(WayangJoin wayangRelNode) {
        Operator childOpLeft = wayangRelConverter.convert(wayangRelNode.getInput(0));
        Operator childOpRight = wayangRelConverter.convert(wayangRelNode.getInput(1));

        RexNode condition = ((Join) wayangRelNode).getCondition();

//        JoinOperator<Tuple2, Tuple2, Integer> join = new JoinOperator(
//                new ProjectionDescriptor<>(
//                        DataUnitType.createBasicUnchecked(Tuple2.class),
//                        DataUnitType.createBasic(Integer.class),
//                        "field0"),
//                new ProjectionDescriptor<>(
//                        DataUnitType.createBasicUnchecked(Tuple2.class),
//                        DataUnitType.createBasic(Integer.class),
//                        "field1"),
//                DataSetType.createDefaultUnchecked(Tuple2.class),
//                DataSetType.createDefaultUnchecked(Tuple2.class));

        JoinOperator<Record, Record, Object> join = new JoinOperator(
                new KeyExtractor0(condition), // pass the index of key for table1
                new KeyExtractor1(condition), // pass the index of key for table2
                Record.class,
                Record.class,
                Object.class);

        childOpLeft.connectTo(0, join, 0); //call connectTo on both operators (left and right)
        childOpRight.connectTo(0, join, 1);

        return join;
    }

    // TODO fix serializable function method
    // extract the left key
    private class KeyExtractor0 implements FunctionDescriptor.SerializableFunction<Record, Object> {

        private final RexNode rexNode;
        private final Integer index;

        private KeyExtractor0(RexNode rexNode) {
            this.rexNode = rexNode;
            RexCall call = (RexCall) rexNode;
            RexNode operand = call.getOperands().get(0);
            RexInputRef rexInputRef = (RexInputRef) operand;
            this.index = rexInputRef.getIndex();
        }

        @Override
        public Object apply(final Record record) {
            System.out.println(record.getField(0).toString());
//            return record.getField(index);
//            return 0;
            return record.getField(0);
        }
    }

    // extract the right key
    private class KeyExtractor1 implements FunctionDescriptor.SerializableFunction<Record, Object> {

        private final RexNode rexNode;
        private final Integer index;

        private KeyExtractor1(RexNode rexNode) {
            this.rexNode = rexNode;
            RexCall call = (RexCall) rexNode;
            RexNode operand = call.getOperands().get(1);
            RexInputRef rexInputRef = (RexInputRef) operand;
            this.index = rexInputRef.getIndex();
        }

        // TODO: index for right table returns index + number of columns in left table
        @Override
        public Object apply(Record record) {
            System.out.println(record.getField(0).toString());
//            return record.getField(index);
//            return 1;
            return record.getField(0);
        }
    }
}
