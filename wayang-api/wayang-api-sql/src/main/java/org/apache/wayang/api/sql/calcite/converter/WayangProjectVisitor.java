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
import org.apache.calcite.sql.SqlOperator;
import org.apache.wayang.api.sql.calcite.rel.WayangProject;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.wayang.core.util.Tuple;
import scala.Tuple1;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BinaryOperator;

public class WayangProjectVisitor extends WayangRelNodeVisitor<WayangProject> {
    WayangProjectVisitor(WayangRelConverter wayangRelConverter) {
        super(wayangRelConverter);
    }

    @Override
    Operator visit(WayangProject wayangRelNode) {

        Operator childOp = wayangRelConverter.convert(wayangRelNode.getInput(0));

        /* Quick check */
        List<RexNode> projects = ((Project) wayangRelNode).getProjects();

        //TODO: create a map with specific dataset type
        MapOperator<Tuple2<Record, Record>, Record> projection = new MapOperator(
                new MapFunctionImpl(projects),
                Record.class,
                Record.class);

        childOp.connectTo(0, projection, 0);

        return projection;
    }


    private class MapFunctionImpl implements
            FunctionDescriptor.SerializableFunction<Record, Record> {

        private final List<RexNode> projects;

        private MapFunctionImpl(List<RexNode> projects) {
            this.projects = projects;
        }

        @Override
        public Record apply(Record record) {

            List<Object> projectedRecord = new ArrayList<>();
            for (int i = 0; i < projects.size(); i++){
                final RexNode exp = projects.get(i);
                if (exp instanceof RexInputRef) {
                    projectedRecord.add(record.getField(((RexInputRef) exp).getIndex()));
                } else if (exp instanceof RexLiteral) {
                    RexLiteral literal = (RexLiteral) exp;
                    projectedRecord.add(literal.getValue());
                } else if (exp instanceof RexCall) {
                    projectedRecord.add(evaluateRexCall(record, (RexCall) exp));
                }
            }
                return new Record(projectedRecord.toArray(new Object[0]));
        }
    }

    public static Object evaluateRexCall(Record record, RexCall rexCall) {
        if (rexCall == null) {
            return null;
        }

        // Get the operator and operands
        SqlOperator operator = rexCall.getOperator();
        List<RexNode> operands = rexCall.getOperands();

        if (operator == SqlStdOperatorTable.PLUS) {
            // Handle addition
            return evaluateNaryOperation(record, operands, Double::sum);
        } else if (operator == SqlStdOperatorTable.MINUS) {
            // Handle subtraction
            return evaluateNaryOperation(record, operands, (a, b) -> a - b);
        } else if (operator == SqlStdOperatorTable.MULTIPLY) {
            // Handle multiplication
            return evaluateNaryOperation(record, operands, (a, b) -> a * b);
        } else if (operator == SqlStdOperatorTable.DIVIDE) {
            // Handle division
            return evaluateNaryOperation(record, operands, (a, b) -> a / b);
        } else {
            return null;
        }
    }

    public static Object evaluateNaryOperation(Record record, List<RexNode> operands, BinaryOperator<Double> operation) {
        if (operands.isEmpty()) {
            return null;
        }

        List<Double> values = new ArrayList<>();

        for (int i = 0; i < operands.size(); i++) {
            Number val = (Number) evaluateRexNode(record, operands.get(i));
            if(val == null){
                return null;
            }
            values.add(val.doubleValue());
        }

        Object result = values.get(0);
        // Perform the operation with the remaining operands
        for (int i = 1; i < operands.size(); i++) {
            result = operation.apply((double)result, values.get(i));
        }

        return result;
    }

    public static Object evaluateRexNode(Record record, RexNode rexNode) {
        if (rexNode instanceof RexCall) {
            // Recursively evaluate a RexCall
            return evaluateRexCall(record, (RexCall) rexNode);
        } else if (rexNode instanceof RexLiteral) {
            // Handle literals (e.g., numbers)
            RexLiteral literal = (RexLiteral) rexNode;
            return literal.getValue();
        } else if (rexNode instanceof RexInputRef) {
            return record.getField(((RexInputRef) rexNode).getIndex());
        }
        else {
            return null; // Unsupported or unknown expression
        }
    }
}