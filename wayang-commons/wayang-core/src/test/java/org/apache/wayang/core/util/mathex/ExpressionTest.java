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

import org.apache.wayang.core.util.mathex.exceptions.EvaluationException;
import org.apache.wayang.core.util.mathex.model.CompiledFunction;
import org.apache.wayang.core.util.mathex.model.Constant;
import org.apache.wayang.core.util.mathex.model.NamedFunction;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test suite for the {@link Expression} subclasses.
 */
class ExpressionTest {

    @Test
    void testSingletonExpressions() {
        DefaultContext context = new DefaultContext(Context.baseContext);
        context.setVariable("x", 42);

        assertEquals(23d, Expression.evaluate("23"), 0d);
        assertEquals(-23d, Expression.evaluate("-23"), 0d);

        assertEquals(42d, Expression.evaluate("x", context), 0d);
        assertEquals(-42d, Expression.evaluate("-x", context), 0d);

        assertEquals(Math.E, Expression.evaluate("e", context), 0d);
        assertEquals(Math.PI, Expression.evaluate("pi", context), 0d);

        assertEquals(2d, Expression.evaluate("log(100, 10)", context), 0.000001);
    }

    @Test
    void testFailsOnMissingContext() {
        Collection<String> faultyExpressions = Arrays.asList(
                "x",
                "myFunction(23)",
                "0 * y",
                "ln(3 * x)"
        );

        for (String faultyExpression : faultyExpressions) {
            boolean isFailed = false;
            try {
                Expression.evaluate(faultyExpression);
            } catch (EvaluationException e) {
                isFailed = true;
            } catch (Throwable t) {
                fail(String.format("Unexpected %s.", t));
            }
            assertTrue(isFailed, String.format("Evaluating \"%s\" did not fail.", faultyExpression));
        }
    }

    @Test
    void testComplexExpressions() {
        {
            final Expression expression = ExpressionBuilder.parse(" (2 *a + 3* b + 5.3 * c0) + 3*abcdef");
            DefaultContext ctx = new DefaultContext();
            ctx.setVariable("a", 5.1);
            ctx.setVariable("b", 3);
            ctx.setVariable("c0", -23);
            ctx.setVariable("abcdef", 821.23);

            assertEquals(
                    (2*5.1 + 3*3 + 5.3*(-23) + 3*821.23),
                    expression.evaluate(ctx),
                    0.0001
            );
        }
    }

    @Test
    void testSpecification() {
        {
            final Expression expression = ExpressionBuilder.parse("ln(x)");
            assertInstanceOf(NamedFunction.class, expression);
            final Expression specifiedExpression = expression.specify(Context.baseContext);
            assertInstanceOf(CompiledFunction.class, specifiedExpression);
        }
        {
            final Expression expression = ExpressionBuilder.parse("ln(e)");
            assertInstanceOf(NamedFunction.class, expression);
            final Expression specifiedExpression = expression.specify(Context.baseContext);
            assertInstanceOf(Constant.class, specifiedExpression);
            assertEquals(1d, specifiedExpression.evaluate(new DefaultContext()), 0.00001d);
        }
    }

}
