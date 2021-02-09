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

package org.apache.wayang.core.util.mathex.model;


import org.apache.wayang.core.util.mathex.Context;
import org.apache.wayang.core.util.mathex.Expression;
import org.apache.wayang.core.util.mathex.exceptions.EvaluationException;

/**
 * An operation {@link Expression}.
 */
public class BinaryOperation implements Expression {

    private final char operator;

    private final Expression operand0, operand1;

    public BinaryOperation(Expression operand0, char operator, Expression operand1) {
        this.operand0 = operand0;
        this.operator = operator;
        this.operand1 = operand1;
    }

    @Override
    public double evaluate(Context context) {
        switch (this.operator) {
            case '+':
                return this.operand0.evaluate(context) + this.operand1.evaluate(context);
            case '-':
                return this.operand0.evaluate(context) - this.operand1.evaluate(context);
            case '*':
                return this.operand0.evaluate(context) * this.operand1.evaluate(context);
            case '/':
                return this.operand0.evaluate(context) / this.operand1.evaluate(context);
            case '%':
                return this.operand0.evaluate(context) % this.operand1.evaluate(context);
            case '^':
                return Math.pow(this.operand0.evaluate(context), this.operand1.evaluate(context));
            default:
                throw new EvaluationException(String.format("Unknown operator: \"%s\"", this.operator));
        }
    }

    @Override
    public Expression specify(Context context) {
        final Expression defaultSpecification = Expression.super.specify(context);
        if (defaultSpecification == this) {
            final Expression specifiedOperand0 = this.operand0.specify(context);
            final Expression specifiedOperand1 = this.operand1.specify(context);
            if (specifiedOperand0 != this.operand0 || specifiedOperand1 != this.operand1) {
                return new BinaryOperation(specifiedOperand0, this.operator, specifiedOperand1);
            }
        }
        return defaultSpecification;
    }

    @Override
    public String toString() {
        return String.format("(%s)%s(%s)", this.operand0, this.operator, this.operand1);
    }

}
