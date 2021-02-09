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

package org.apache.wayang.core.plan.wayangplan;

import org.junit.Assert;
import org.junit.Test;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.WayangCollections;

import java.util.Collection;

/**
 * Test suite for the {@link Operator} class.
 */
public class OperatorTest {

    public class Operator1 extends OperatorBase {

        @EstimationContextProperty
        protected final double op1Property;

        protected final double someOtherProperty = 0d;

        public Operator1(double op1Property) {
            super(0, 0, false);
            this.op1Property = op1Property;
        }

        public double getOp1Property() {
            return this.op1Property;
        }

    }

    public class Operator2 extends Operator1 {

        @EstimationContextProperty
        private final double op2Property;

        public Operator2(double op1Property, double op2Property) {
            super(op1Property);
            this.op2Property = op2Property;
        }

        public double getOp2Property() {
            return this.op2Property;
        }
    }

    @Test
    public void testPropertyDetection() {
        Operator op = new Operator2(0, 1);
        final Collection<String> estimationContextProperties = op.getEstimationContextProperties();
        Assert.assertEquals(
                WayangCollections.asSet("op1Property", "op2Property"),
                WayangCollections.asSet(estimationContextProperties)
        );
    }

    @Test
    public void testPropertyCollection() {
        Operator op = new Operator2(0, 1);
        Assert.assertEquals(
                Double.valueOf(0d),
                ReflectionUtils.getProperty(op, "op1Property")
        );
        Assert.assertEquals(
                Double.valueOf(1d),
                ReflectionUtils.getProperty(op, "op2Property")
        );
    }


}
