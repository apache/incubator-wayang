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
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableBiFunction;
import org.apache.wayang.basic.data.Record;

public class ProjectMapFuncImpl implements
        FunctionDescriptor.SerializableFunction<Record, Record> {

    /**
     * Serializable representation of {@link RexNode}
     */
    abstract class Node implements Serializable {
        public Node transform(final RexNode node) {
            if (node instanceof RexCall) {
                return new Call((RexCall) node);
            } else if (node instanceof RexInputRef) {
                return new InputRef((RexInputRef) node);
            } else if (node instanceof RexLiteral) {
                return new Literal((RexLiteral) node);
            } else {
                throw new UnsupportedOperationException("RexNode not supported in projection function: " + node);
            }
        }

        abstract Object evaluate(Record record);
    }

    /**
     * Serializable representation of {@link RexCall}
     */
    class Call extends Node {
        final SerializableBiFunction<Number, Number, Number> operation;
        final List<Node> children;

        public Call(final RexCall call) {
            this.operation = deriveOperation(call.getOperator().getKind());
            this.children = call.getOperands().stream()
                    .map(op -> this.transform(op))
                    .collect(Collectors.toList());
        }

        public Object evaluate(final Record record) {
            assert (children.size() == 2) : "Project func call should only have two children";
            return operation.apply((Number) children.get(0).evaluate(record),
                    (Number) children.get(1).evaluate(record));
        }

        /**
         * Derives the java operator for a given {@link SqlKind}, and turns it into a serializable function
         * @param kind {@link SqlKind} from {@link RexCall} SqlOperator
         * @return a serializable function of +, -, * or /
         * @throws UnsupportedOperationException on unrecognized {@link SqlKind}
         */
        static SerializableBiFunction<Number, Number, Number> deriveOperation(final SqlKind kind) {
            return (a, b) -> {
                final double l = a.doubleValue();
                final double r = b.doubleValue();
                switch (kind) {
                    case PLUS:
                        return l + r;
                    case MINUS:
                        return l - r;
                    case TIMES:
                        return l * r;
                    case DIVIDE:
                        return l / r;
                    default:
                        throw new UnsupportedOperationException(
                                "Operation not supported in projection function RexCall: " + kind);
                }
            };
        }
    }

    /**
     * Serializable representation of {@link RexLiteral}
     */
    class Literal extends Node {
        final Comparable<?> value;

        Literal(final RexLiteral literal) {
            this.value = literal.getValueAs(Double.class);
        }

        @Override
        public Object evaluate(final Record record) {
            return value;
        }
    }

    /**
     * Serializable representation of {@link InputRef}
     */
    class InputRef extends Node {
        final int key;

        InputRef(final RexInputRef inputRef) {
            this.key = inputRef.getIndex();
        }

        @Override
        public Object evaluate(final Record record) {
            return record.getField(key);
        }
    }

    /**
     * AST of the {@link RexCall} arithmetic, composed into serializable nodes; {@link Call}, {@link InputRef}, {@link Literal}
     */
    final List<Node> projectionSyntaxTrees;

    public ProjectMapFuncImpl(final List<RexNode> projects) {
        this.projectionSyntaxTrees = projects.stream()
                .map(projection -> {
                    if (projection instanceof RexCall) {
                        return new Call((RexCall) projection);
                    } else if (projection instanceof RexLiteral) {
                        return new Literal((RexLiteral) projection);
                    } else if (projection instanceof RexInputRef) {
                        return new InputRef((RexInputRef) projection);
                    } else {
                        throw new UnsupportedOperationException("RexNode not supported in projection: " + projection);
                    }
                })
                .collect(Collectors.toList());
    }

    @Override
    public Record apply(final Record record) {
        return new Record(projectionSyntaxTrees.stream().map(call -> call.evaluate(record)).toArray());
    }
}
