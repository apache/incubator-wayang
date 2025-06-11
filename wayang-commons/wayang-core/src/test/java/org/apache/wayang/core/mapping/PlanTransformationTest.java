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

import org.apache.wayang.core.mapping.test.TestSinkToTestSink2Factory;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OperatorAlternative;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.plan.wayangplan.UnarySink;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.plan.wayangplan.test.TestSink;
import org.apache.wayang.core.plan.wayangplan.test.TestSink2;
import org.apache.wayang.core.plan.wayangplan.test.TestSource;
import org.apache.wayang.core.test.TestDataUnit;
import org.apache.wayang.core.types.DataSetType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Test suite for the {@link org.apache.wayang.core.mapping.PlanTransformation} class.
 */
class PlanTransformationTest {

    @Test
    void testReplace() {
        // Build the plan.
        UnarySource source = new TestSource(DataSetType.createDefault(TestDataUnit.class));
        UnarySink sink = new TestSink(DataSetType.createDefault(TestDataUnit.class));
        source.connectTo(0, sink, 0);
        WayangPlan plan = new WayangPlan();
        plan.addSink(sink);

        // Build the pattern.
        OperatorPattern sinkPattern = new OperatorPattern("sink", new TestSink(DataSetType.createDefault(TestDataUnit.class)), false);
        SubplanPattern subplanPattern = SubplanPattern.createSingleton(sinkPattern);

        // Build the replacement strategy.
        ReplacementSubplanFactory replacementSubplanFactory = new TestSinkToTestSink2Factory();
        PlanTransformation planTransformation = new PlanTransformation(subplanPattern, replacementSubplanFactory).thatReplaces();

        // Apply the replacement strategy to the graph.
        planTransformation.transform(plan, Operator.FIRST_EPOCH + 1);

        // Check the correctness of the transformation.
        assertEquals(1, plan.getSinks().size());
        final Operator replacedSink = plan.getSinks().iterator().next();
        assertInstanceOf(TestSink2.class, replacedSink);
        assertEquals(source, replacedSink.getEffectiveOccupant(0).getOwner());
    }

    @Test
    void testIntroduceAlternative() {
        // Build the plan.
        UnarySource source = new TestSource(DataSetType.createDefault(TestDataUnit.class));
        UnarySink sink = new TestSink(DataSetType.createDefault(TestDataUnit.class));
        source.connectTo(0, sink, 0);
        WayangPlan plan = new WayangPlan();
        plan.addSink(sink);

        // Build the pattern.
        OperatorPattern sinkPattern = new OperatorPattern("sink", new TestSink(DataSetType.createDefault(TestDataUnit.class)), false);
        SubplanPattern subplanPattern = SubplanPattern.createSingleton(sinkPattern);

        // Build the replacement strategy.
        ReplacementSubplanFactory replacementSubplanFactory = new TestSinkToTestSink2Factory();
        PlanTransformation planTransformation = new PlanTransformation(subplanPattern, replacementSubplanFactory);

        // Apply the replacement strategy to the graph.
        planTransformation.transform(plan, Operator.FIRST_EPOCH + 1);

        // Check the correctness of the transformation.
        assertEquals(1, plan.getSinks().size());
        final Operator replacedSink = plan.getSinks().iterator().next();
        assertInstanceOf(OperatorAlternative.class, replacedSink);
        OperatorAlternative operatorAlternative = (OperatorAlternative) replacedSink;
        assertEquals(2, operatorAlternative.getAlternatives().size());
        assertInstanceOf(TestSink.class, operatorAlternative.getAlternatives().get(0).getContainedOperator());
        assertInstanceOf(TestSink2.class, operatorAlternative.getAlternatives().get(1).getContainedOperator());
        assertEquals(source, replacedSink.getEffectiveOccupant(0).getOwner());
    }

    @Test
    void testFlatAlternatives() {
        // Build the plan.
        UnarySource source = new TestSource(DataSetType.createDefault(TestDataUnit.class));
        UnarySink sink = new TestSink(DataSetType.createDefault(TestDataUnit.class));
        source.connectTo(0, sink, 0);
        WayangPlan plan = new WayangPlan();
        plan.addSink(sink);

        // Build the pattern.
        OperatorPattern sinkPattern = new OperatorPattern("sink", new TestSink(DataSetType.createDefault(TestDataUnit.class)), false);
        SubplanPattern subplanPattern = SubplanPattern.createSingleton(sinkPattern);

        // Build the replacement strategy.
        ReplacementSubplanFactory replacementSubplanFactory = new TestSinkToTestSink2Factory();
        PlanTransformation planTransformation = new PlanTransformation(subplanPattern, replacementSubplanFactory);

        // Apply the replacement strategy to the graph twice.
        planTransformation.transform(plan, Operator.FIRST_EPOCH + 1);
        planTransformation.transform(plan, Operator.FIRST_EPOCH + 1);

        // Check the correctness of the transformation.
        assertEquals(1, plan.getSinks().size());
        final Operator replacedSink = plan.getSinks().iterator().next();
        assertInstanceOf(OperatorAlternative.class, replacedSink);
        OperatorAlternative operatorAlternative = (OperatorAlternative) replacedSink;
        assertEquals(3, operatorAlternative.getAlternatives().size());
        assertInstanceOf(TestSink.class, operatorAlternative.getAlternatives().get(0).getContainedOperator());
        assertInstanceOf(TestSink2.class, operatorAlternative.getAlternatives().get(1).getContainedOperator());
        assertInstanceOf(TestSink2.class, operatorAlternative.getAlternatives().get(2).getContainedOperator());
        assertEquals(source, replacedSink.getEffectiveOccupant(0).getOwner());
    }

}
