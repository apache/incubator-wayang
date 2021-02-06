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

package org.apache.wayang.core.mapping;

import org.junit.Assert;
import org.junit.Test;
import org.apache.wayang.core.test.DummyExecutionOperator;

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
