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

package org.apache.wayang.core.util;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

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
        final Object val = ReflectionUtils.evaluate("org.apache.wayang.core.util.ReflectionUtilsTest.TEST_INT");
        Assert.assertEquals(42, val);
    }

    @Test
    public void testEvaluateWithStaticMethod() {
        final Object val = ReflectionUtils.evaluate("org.apache.wayang.core.util.ReflectionUtilsTest.testInt()");
        Assert.assertEquals(23, val);
    }

    public interface MyParameterizedInterface<A> { }

    public static class MyParameterizedClassA<A, B> {
    }

    public static class MyParameterizedClassB<C, D> extends MyParameterizedClassA<C, D> {
    }

    public static class MyParameterizedClassC implements MyParameterizedInterface<Long> { }


    @Test
    public void testGetTypeParametersWithReusedTypeParameters() {
        MyParameterizedClassA<Integer, String> myParameterizedObject = new MyParameterizedClassB<Integer, String>() {
        };
        Map<String, Type> expectedTypeParameters = new HashMap<>();
        expectedTypeParameters.put("A", Integer.class);
        expectedTypeParameters.put("B", String.class);

        final Map<String, Type> typeParameters = ReflectionUtils.getTypeParameters(
                myParameterizedObject.getClass(),
                MyParameterizedClassA.class
        );

        Assert.assertEquals(expectedTypeParameters, typeParameters);
    }

    @Test
    public void testGetTypeParametersWithIndirectTypeParameters() {
        MyParameterizedClassC myParameterizedObject = new MyParameterizedClassC() {
        };
        Map<String, Type> expectedTypeParameters = new HashMap<>();
        expectedTypeParameters.put("A", Long.class);

        final Map<String, Type> typeParameters = ReflectionUtils.getTypeParameters(
                myParameterizedObject.getClass(),
                MyParameterizedInterface.class
        );

        Assert.assertEquals(expectedTypeParameters, typeParameters);
    }

}
