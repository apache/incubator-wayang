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

package org.apache.wayang.api.sql.calcite.converter.functions;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.rex.RexVisitorImpl;

import org.apache.wayang.api.sql.calcite.converter.WayangFilterVisitor;
import org.apache.wayang.basic.data.Record;

public class FilterEvaluateCondition extends RexVisitorImpl<Boolean> {
    final Record record;

    protected FilterEvaluateCondition(final boolean deep, final Record record) {
        super(deep);
        this.record = record;
    }

    @Override
    public Boolean visitCall(final RexCall call) {
        final SqlKind kind = call.getKind();

        if (!kind.belongsTo(WayangFilterVisitor.SUPPORTED_OPS))
            throw new IllegalStateException(
                    "Cannot handle this filter predicate yet: " + kind + " during RexCall: " + call);

        switch (kind) {
            // Since NOT captures only one operand we just get
            // the first
            case NOT:
                assert (call.getOperands().size() == 1) : "SqlKind.NOT should only have 1 operand in call got: "
                        + call.getOperands().size() + ", call: " + call;
                return !(call.getOperands().get(0).accept(this));
            case AND:
                return call.getOperands().stream().allMatch(operator -> operator.accept(this));
            case OR:
                return call.getOperands().stream().anyMatch(operator -> operator.accept(this));
            default:
                assert (call.getOperands().size() == 2);
                return eval(record, kind, call.getOperands().get(0), call.getOperands().get(1));
        }
    }

    public boolean eval(final Record record, final SqlKind kind, final RexNode leftOperand,
            final RexNode rightOperand) {
        if (leftOperand instanceof RexInputRef && rightOperand instanceof RexLiteral) {
            final RexInputRef rexInputRef = (RexInputRef) leftOperand;
            final int index = rexInputRef.getIndex();
            final Object field = record.getField(index);
            final RexLiteral rexLiteral = (RexLiteral) rightOperand;

            switch (kind) {
                case LIKE:
                    return SqlFunctions.like(field.toString(), rexLiteral.toString().replace("'", ""));
                case GREATER_THAN:
                    return isGreaterThan(field, rexLiteral);
                case LESS_THAN:
                    return isLessThan(field, rexLiteral);
                case EQUALS:
                    return isEqualTo(field, rexLiteral);
                case GREATER_THAN_OR_EQUAL:
                    return isGreaterThan(field, rexLiteral) || isEqualTo(field, rexLiteral);
                case LESS_THAN_OR_EQUAL:
                    return isLessThan(field, rexLiteral) || isEqualTo(field, rexLiteral);
                default:
                    throw new IllegalStateException("Predicate not supported yet: " + kind);
            }
        } else if (leftOperand instanceof RexInputRef && rightOperand instanceof RexInputRef) {
            final RexInputRef leftRexInputRef = (RexInputRef) leftOperand;
            final int leftIndex = leftRexInputRef.getIndex();
            final RexInputRef righRexInputRef = (RexInputRef) rightOperand;
            final int rightIndex = righRexInputRef.getIndex();

            switch (kind) {
                case EQUALS:
                    return isEqualTo(record.getField(leftIndex), record.getField(rightIndex));
                default:
                    throw new IllegalStateException("Predicate not supported yet, kind: " + kind + " left field: "
                            + record.getField(leftIndex) + " right field: " + record.getField(rightIndex));
            }
        } else {
            throw new IllegalStateException("Predicate not supported with types yet, predicate: " + kind + ", type1: "
                    + leftOperand.getClass() + ", type2: " + rightOperand.getClass());
        }
    }

    private boolean isGreaterThan(final Object o, final RexLiteral rexLiteral) {
        // return rexLiteral.getValue().compareTo(o)< 0;
        return ((Comparable) o).compareTo(rexLiteral.getValueAs(o.getClass())) > 0;

    }

    private boolean isLessThan(final Object o, final RexLiteral rexLiteral) {
        return ((Comparable) o).compareTo(rexLiteral.getValueAs(o.getClass())) < 0;
    }

    private boolean isEqualTo(final Object o, final RexLiteral rexLiteral) {
        try {
            return ((Comparable) o).compareTo(rexLiteral.getValueAs(o.getClass())) == 0;
        } catch (final Exception e) {
            throw new IllegalStateException("Predicate not supported yet");
        }
    }

    private boolean isEqualTo(final Object o1, final Object o2) {
        System.out.println("comparing: " + o1 + " with " + o2);
        System.out.println("true: " + ((Comparable) o1).compareTo(o2));
        try {
            return ((Comparable) o1).compareTo(o2) == 0;
        } catch (final Exception e) {
            throw new IllegalStateException("Predicate not supported yet");
        }
    }
}
