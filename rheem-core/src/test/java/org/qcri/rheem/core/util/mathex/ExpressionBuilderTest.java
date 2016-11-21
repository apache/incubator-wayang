package org.qcri.rheem.core.util.mathex;


import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.util.mathex.exceptions.ParseException;

import java.util.Arrays;
import java.util.Collection;

/**
 * Test suite for the class {@link ExpressionBuilder}.
 */
public class ExpressionBuilderTest {

    @Test
    public void shouldNotFailOnValidInput() {
        Collection<String> expressions = Arrays.asList(
                "x",
                "f(x)",
                "1 + 2 + -3",
                "1 + 2 * 3",
                "1 - 2 + 3",
                "x + 1 + 3*f(x, 3^2)",
                "(2 *a + 3* b + 5.3 * c0) + 3*abcdef"
        );
        for (String expression : expressions) {
            ExpressionBuilder.parse(expression);
        }
    }

    @Test
    public void shouldFailOnInvalidInput() {
        Collection<String> expressions = Arrays.asList(
                "2x",
                "f(x,)",
                "~3",
                "",
                "*2",
                "f(3, x"
        );
        for (String expression : expressions) {
            boolean isFailed = false;
            try {
                ExpressionBuilder.parse(expression);
            } catch (ParseException e) {
                isFailed = true;
            } finally {
                Assert.assertTrue(isFailed);
            }
        }
    }

}
