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

import org.junit.Assert;
import org.junit.Test;
import org.apache.wayang.core.util.mathex.exceptions.EvaluationException;
import org.apache.wayang.core.util.mathex.model.CompiledFunction;
import org.apache.wayang.core.util.mathex.model.Constant;
import org.apache.wayang.core.util.mathex.model.NamedFunction;

import java.util.Arrays;
import java.util.Collection;

/**
 * Test suite for the {@link Expression} subclasses.
 */
public class ExpressionTest {

    @Test
    public void testSingletonExpressions() {
        DefaultContext context = new DefaultContext(Context.baseContext);
        context.setVariable("x", 42);

        Assert.assertEquals(23d, Expression.evaluate("23"), 0d);
        Assert.assertEquals(-23d, Expression.evaluate("-23"), 0d);

        Assert.assertEquals(42d, Expression.evaluate("x", context), 0d);
        Assert.assertEquals(-42d, Expression.evaluate("-x", context), 0d);

        Assert.assertEquals(Math.E, Expression.evaluate("e", context), 0d);
        Assert.assertEquals(Math.PI, Expression.evaluate("pi", context), 0d);

        Assert.assertEquals(2d, Expression.evaluate("log(100, 10)", context), 0.000001);
    }

    @Test
    public void testFailsOnMissingContext() {
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
                Assert.fail(String.format("Unexpected %s.", t));
            }
            Assert.assertTrue(String.format("Evaluating \"%s\" did not fail.", faultyExpression), isFailed);
        }
    }

    @Test
    public void testComplexExpressions() {
        {
            final Expression expression = ExpressionBuilder.parse(" (2 *a + 3* b + 5.3 * c0) + 3*abcdef");
            DefaultContext ctx = new DefaultContext();
            ctx.setVariable("a", 5.1);
            ctx.setVariable("b", 3);
            ctx.setVariable("c0", -23);
            ctx.setVariable("abcdef", 821.23);

            Assert.assertEquals(
                    (2*5.1 + 3*3 + 5.3*(-23) + 3*821.23),
                    expression.evaluate(ctx),
                    0.0001
            );
        }
    }

    @Test
    public void testSpecification() {
        {
            final Expression expression = ExpressionBuilder.parse("ln(x)");
            Assert.assertTrue(expression instanceof NamedFunction);
            final Expression specifiedExpression = expression.specify(Context.baseContext);
            Assert.assertTrue(specifiedExpression instanceof CompiledFunction);
        }
        {
            final Expression expression = ExpressionBuilder.parse("ln(e)");
            Assert.assertTrue(expression instanceof NamedFunction);
            final Expression specifiedExpression = expression.specify(Context.baseContext);
            Assert.assertTrue(specifiedExpression instanceof Constant);
            Assert.assertEquals(1d, specifiedExpression.evaluate(new DefaultContext()), 0.00001d);
        }
    }

}
