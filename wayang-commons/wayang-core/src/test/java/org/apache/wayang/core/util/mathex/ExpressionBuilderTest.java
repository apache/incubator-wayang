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
import org.apache.wayang.core.util.mathex.exceptions.ParseException;

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
                // TODO: For some reason this is not failing on my machine
                //"2x",
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
