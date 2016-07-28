package org.qcri.rheem.core.mapping;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.test.DummyExecutionOperator;

/**
 * Tests for {@link OperatorPattern}.
 */
public class OperatorPatternTest {

    @Test
    public void testAdditionalTests() {
        DummyExecutionOperator operator1 = new DummyExecutionOperator(1, 1, true);
        DummyExecutionOperator operator2 = new DummyExecutionOperator(1, 1, true);
        OperatorPattern<DummyExecutionOperator> pattern = new OperatorPattern<>("test", operator1, false);

        // The pattern should, of course, now match the operators.
        Assert.assertNotNull(pattern.match(operator1));
        Assert.assertNotNull(pattern.match(operator2));

        // The following test now should restrict the matching operators to operator1.
        pattern.withAdditionalTest(op -> op == operator1);
        Assert.assertNotNull(pattern.match(operator1));
        Assert.assertNull(pattern.match(operator2));
    }

}