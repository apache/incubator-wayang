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

package org.apache.wayang.core.util.mathex;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.apache.wayang.core.mathex.MathExBaseVisitor;
import org.apache.wayang.core.mathex.MathExLexer;
import org.apache.wayang.core.mathex.MathExParser;
import org.apache.wayang.core.util.mathex.exceptions.ParseException;
import org.apache.wayang.core.util.mathex.model.BinaryOperation;
import org.apache.wayang.core.util.mathex.model.Constant;
import org.apache.wayang.core.util.mathex.model.NamedFunction;
import org.apache.wayang.core.util.mathex.model.UnaryOperation;
import org.apache.wayang.core.util.mathex.model.Variable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * This utility builds {@link Expression}s from an input {@link String}.
 */
public class ExpressionBuilder extends MathExBaseVisitor<Expression> {

    /**
     * Parse the {@code specification} and construct an {@link Expression} from it.
     *
     * @param specification a mathematical expression
     * @return the constructed {@link Expression}
     * @throws ParseException if the expression could not be parsed properly
     */
    public static Expression parse(String specification) throws ParseException {
        MathExLexer lexer = new MathExLexer(new ANTLRInputStream(specification));
        lexer.removeErrorListeners();
        lexer.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
                throw new ParseException("Syntax error.", e);
            }
        });
        MathExParser parser = new MathExParser(new CommonTokenStream(lexer));

        // Suppress console output on errors. Throw exceptions instead.
        parser.removeErrorListeners();
        parser.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object o, int i, int i1, String s, RecognitionException e) {
                throw new ParseException("Syntax error.", e);
            }

        });
        MathExParser.ExpressionContext expressionContext = parser.expression();

        return new ExpressionBuilder().visit(expressionContext);
    }

    @Override
    public Expression visitConstant(MathExParser.ConstantContext ctx) {
        return new Constant(Double.parseDouble(ctx.value.getText()));
    }

    @Override
    public Expression visitFunction(MathExParser.FunctionContext ctx) {
        // Get the function name.
        final String functionName = ctx.name.getText();

        // Get the parameter expressions.
        final List<MathExParser.ExpressionContext> argExpressions = ctx.expression();
        final List<Expression> args = argExpressions.stream()
                .map(this::visit)
                .collect(Collectors.toList());

        return new NamedFunction(functionName, args);
    }

    @Override
    public Expression visitVariable(MathExParser.VariableContext ctx) {
        return new Variable(ctx.getText());
    }

    @Override
    public Expression visitParensExpression(MathExParser.ParensExpressionContext ctx) {
        return super.visitParensExpression(ctx);
    }

    @Override
    public Expression visitBinaryOperation(MathExParser.BinaryOperationContext ctx) {
        final char operator = ctx.operator.getText().charAt(0);
        final Expression operand0 = this.visit(ctx.operand0);
        final Expression operand1 = this.visit(ctx.operand1);

        return new BinaryOperation(operand0, operator, operand1);
    }

    @Override
    public Expression visitUnaryOperation(MathExParser.UnaryOperationContext ctx) {
        final char operator = ctx.operator.getText().charAt(0);
        final Expression operand = this.visit(ctx.expression());
        return new UnaryOperation(operator, operand);
    }

    @Override
    protected Expression aggregateResult(Expression aggregate, Expression nextResult) {
        if (aggregate != null && nextResult != null) {
            throw new ParseException("Parsing logic defect.");
        }
        return aggregate == null ? nextResult : aggregate;
    }
}
