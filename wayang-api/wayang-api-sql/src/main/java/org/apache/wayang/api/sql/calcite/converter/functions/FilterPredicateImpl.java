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

import java.io.Serializable;
import java.util.Arrays;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlKind;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;

public class FilterPredicateImpl implements FunctionDescriptor.SerializablePredicate<Record> {
    private final Node callTree;

    public FilterPredicateImpl(final RexNode condition) {
        assert (condition instanceof RexCall) : "Condition was not a RexCall expression: " + condition;
        this.callTree = new Call((RexCall) condition);
    }

    @Override
    public boolean test(final Record record) {
        return (boolean) callTree.evaluate(record);
    }

    private Node transform(RexNode node) {
        if (node instanceof RexCall) {
            return new Call((RexCall) node);
        } else if (node instanceof RexInputRef) {
            return new InputRef((RexInputRef) node);
        } else if (node instanceof RexLiteral) {
            return new Literal((RexLiteral) node);
        } else {
            throw new UnsupportedOperationException("Unsupported RexNode in filter condition: " + node);
        }
    }

    abstract class Node implements Serializable {
        abstract Object evaluate(final Record record);
    }

    class Call extends Node {
        final Node[] operands;
        final SerializableFunction<Object[], Object> operation;

        Call(final RexCall call) {
            operands = call.getOperands().stream().map(op -> transform(op)).toArray(Node[]::new);
            operation = deriveOperation(call.getOperator().getKind());
        }

        @Override
        Object evaluate(final Record record) {
            return operation.apply(Arrays.stream(operands).map(op -> op.evaluate(record)).toArray());
        }

        SerializableFunction<Object[], Object> deriveOperation(SqlKind kind) {
            switch (kind) {
                case NOT:
                    assert (operands.length == 1)
                            : "Expected operation " + kind + " to have 1 operand but got: " + operands.length;
                    return input -> !(boolean) input[0];
                case IS_NOT_NULL:
                    assert (operands.length == 1)
                            : "Expected operation " + kind + " to have 1 operand but got: " + operands.length;
                    return input -> !isEqualTo(input[0], null);
                case IS_NULL:
                    assert (operands.length == 1)
                            : "Expected operation " + kind + " to have 1 operand but got: " + operands.length;
                    return input -> isEqualTo(input[0], null);
                case LIKE:
                    assert (operands.length == 2)
                            : "Expected operation " + kind + " to have 2 operands but got: " + operands.length;
                    return input -> like((String) input[0], (String) input[1]);
                case NOT_EQUALS:
                    assert (operands.length == 2)
                            : "Expected operation " + kind + " to have 2 operands but got: " + operands.length;
                    return input -> !isEqualTo(input[0], input[1]);
                case EQUALS:
                    assert (operands.length == 2)
                            : "Expected operation " + kind + " to have 2 operands but got: " + operands.length;
                    return input -> isEqualTo(input[0], input[1]);
                case GREATER_THAN:
                    assert (operands.length == 2)
                            : "Expected operation " + kind + " to have 2 operands but got: " + operands.length;
                case LESS_THAN:
                    assert (operands.length == 2)
                            : "Expected operation " + kind + " to have 2 operands but got: " + operands.length;
                    return input -> isLessThan(input[0], input[1]);
                case GREATER_THAN_OR_EQUAL:
                    assert (operands.length == 2)
                            : "Expected operation " + kind + " to have 2 operands but got: " + operands.length;
                    return input -> isGreaterThan(input[0], input[1]) || isEqualTo(input[0], input[1]);
                case LESS_THAN_OR_EQUAL:
                    assert (operands.length == 2)
                            : "Expected operation " + kind + " to have 2 operands but got: " + operands.length;
                    return input -> isLessThan(input[0], input[1]) || isEqualTo(input[0], input[1]);
                case AND:
                    assert (operands.length > 1)
                            : "Expected at least two operands for " + kind + " , got: " + operands.length;
                    return input -> Arrays.stream(input).map(Boolean.class::cast).allMatch(Boolean::booleanValue);
                case OR:
                    assert (operands.length > 1)
                            : "Expected at least two operands for " + kind + " , got: " + operands.length;
                    return input -> Arrays.stream(input).map(Boolean.class::cast).anyMatch(Boolean::booleanValue);
                default:
                    throw new UnsupportedOperationException("Kind not supported: " + kind);
            }
        }

    }

    class Literal extends Node {
        final Object value;

        Literal(final RexLiteral literal) {
            value = literal.getValue2();
        }

        @Override
        Object evaluate(final Record record) {
            return value;
        }
    }

    class InputRef extends Node {
        final int key;

        InputRef(final RexInputRef inputRef) {
            this.key = inputRef.getIndex();
        }

        @Override
        Object evaluate(final Record record) {
            return record.getField(key);
        }
    }

    private boolean like(final String s1, final String s2) {
        final SqlFunctions.LikeFunction likeFunction = new SqlFunctions.LikeFunction();
        final boolean isMatch = likeFunction.like(s1, s2);

        return isMatch;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private boolean isGreaterThan(final Object o1, final Object o2) {
        assert (o1 instanceof Comparable);
        return ((Comparable) o1).compareTo(o2) > 0;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private boolean isLessThan(final Object o1, final Object o2) {
        assert (o1 instanceof Comparable);
        return ((Comparable) o1).compareTo(o2) < 0;
    }

    @SuppressWarnings("rawtypes")
    private boolean isEqualTo(final Object o1, final Object o2) {
        assert (o1 instanceof Comparable);
        return ((Comparable) o1).equals(o2);
    }
}