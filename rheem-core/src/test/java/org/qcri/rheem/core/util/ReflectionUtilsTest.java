package org.qcri.rheem.core.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test suite for {@link ReflectionUtils}.
 */
public class ReflectionUtilsTest {

    @SuppressWarnings("unused")
    public static int TEST_INT = 42;

    @SuppressWarnings("unused")
    public static int testInt() {
        return 23;
    }

    @Test
    public void testEvaluateWithInstantiation() {
        final Object val = ReflectionUtils.evaluate("new java.lang.String()");
        Assert.assertEquals("", val);
    }

    @Test
    public void testEvaluateWithStaticVariable() {
        final Object val = ReflectionUtils.evaluate("org.qcri.rheem.core.util.ReflectionUtilsTest.TEST_INT");
        Assert.assertEquals(42, val);
    }

    @Test
    public void testEvaluateWithStaticMethod() {
        final Object val = ReflectionUtils.evaluate("org.qcri.rheem.core.util.ReflectionUtilsTest.testInt()");
        Assert.assertEquals(23, val);
    }

}