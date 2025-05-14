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

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;

/**
 * AST of the {@link RexCall} arithmetic, composed into serializable nodes;
 * {@link Call}, {@link InputRef}, {@link Literal}
 */
interface CallTreeFactory<Input, Output> extends Serializable {
    public default Node<Output> fromRexNode(final RexNode node) {
        if (node instanceof RexCall) {
            return new Call<>((RexCall) node, this);
        } else if (node instanceof RexInputRef) {
            return new InputRef<>((RexInputRef) node);
        } else if (node instanceof RexLiteral) {
            return new Literal<>((RexLiteral) node);
        } else {
            throw new UnsupportedOperationException("Unsupported RexNode in filter condition: " + node);
        }
    }

    /**
     * Derives the java operator for a given {@link SqlKind}, and turns it into a
     * serializable function
     * 
     * @param kind {@link SqlKind} from {@link RexCall} SqlOperator
     * @return a serializable function of +, -, * or /
     * @throws UnsupportedOperationException on unrecognized {@link SqlKind}
     */
    public SerializableFunction<List<Output>, Output> deriveOperation(SqlKind kind);
}

interface Node<Output> extends Serializable {
    public Output evaluate(final Record record);
}

class Call<Input, Output> implements Node<Output> {
    final List<Node<Output>> operands;
    final SerializableFunction<List<Output>, Output> operation;

    protected Call(final RexCall call, final CallTreeFactory<Input, Output> tree) {
        operands = call.getOperands().stream().map(tree::fromRexNode).collect(Collectors.toList());
        operation = tree.deriveOperation(call.getKind());
    }

    @Override
    public Output evaluate(final Record record) {
        return operation.apply(operands.stream().map(op -> op.evaluate(record)).collect(Collectors.toList()));
    }
}

class Literal<Output> implements Node<Output> {
    final Output value;

    Literal(final RexLiteral literal) {
        value = (Output) literal.getValue2();
    }

    @Override
    public Output evaluate(final Record record) {
        return value;
    }
}

class InputRef<Output> implements Node<Output> {
    private final int key;

    InputRef(final RexInputRef inputRef) {
        this.key = inputRef.getIndex();
    }

    @Override
    public Output evaluate(final Record record) {
        return (Output) record.getField(key);
    }
}