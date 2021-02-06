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
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.plan.wayangplan.UnarySink;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.plan.wayangplan.test.TestSink;
import org.apache.wayang.core.plan.wayangplan.test.TestSource;
import org.apache.wayang.core.test.TestDataUnit;
import org.apache.wayang.core.types.DataSetType;

import java.util.List;

/**
 * Test suite for the {@link SubplanPattern}.
 */
public class SubplanPatternTest {

    @Test
    public void testMatchSinkPattern() {
        // Build the plan.
        UnarySource source = new TestSource(DataSetType.createDefault(TestDataUnit.class));
        UnarySink sink = new TestSink(DataSetType.createDefault(TestDataUnit.class));
        source.connectTo(0, sink, 0);
        WayangPlan plan = new WayangPlan();
        plan.addSink(sink);

        // Build the pattern.
        OperatorPattern sinkPattern = new OperatorPattern("sink", new TestSink(DataSetType.createDefault(TestDataUnit.class)), false);
        SubplanPattern subplanPattern = SubplanPattern.createSingleton(sinkPattern);

        // Match the pattern against the plan.
        final List<SubplanMatch> matches = subplanPattern.match(plan, Operator.FIRST_EPOCH);

        // Evaluate the matches.
        Assert.assertEquals(1, matches.size());
        final SubplanMatch match = matches.get(0);
        Assert.assertEquals(sink, match.getOperatorMatches().get("sink").getOperator());
    }

    @Test
    public void testMatchSourcePattern() {
        // Build the plan.
        UnarySource source = new TestSource(DataSetType.createDefault(TestDataUnit.class));
        UnarySink sink = new TestSink(DataSetType.createDefault(TestDataUnit.class));
        source.connectTo(0, sink, 0);
        WayangPlan plan = new WayangPlan();
        plan.addSink(sink);

        // Build the pattern.
        OperatorPattern sourcePattern = new OperatorPattern("source", new TestSource(DataSetType.createDefault(TestDataUnit.class)), false);
        SubplanPattern subplanPattern = SubplanPattern.createSingleton(sourcePattern);

        // Match the pattern against the plan.
        final List<SubplanMatch> matches = subplanPattern.match(plan, Operator.FIRST_EPOCH);

        // Evaluate the matches.
        Assert.assertEquals(1, matches.size());
        final SubplanMatch match = matches.get(0);
        Assert.assertEquals(source, match.getOperatorMatches().get("source").getOperator());
    }

    @Test
    public void testMatchChainedPattern() {
        // Build the plan.
        UnarySource source = new TestSource(DataSetType.createDefault(TestDataUnit.class));
        UnarySink sink = new TestSink(DataSetType.createDefault(TestDataUnit.class));
        source.connectTo(0, sink, 0);
        WayangPlan plan = new WayangPlan();
        plan.addSink(sink);

        // Build the pattern.
        OperatorPattern sourcePattern = new OperatorPattern("source", new TestSource(DataSetType.createDefault(TestDataUnit.class)), false);
        OperatorPattern sinkPattern = new OperatorPattern("sink", new TestSink(DataSetType.createDefault(TestDataUnit.class)), false);
        sourcePattern.connectTo(0, sinkPattern, 0);
        SubplanPattern subplanPattern = SubplanPattern.fromOperatorPatterns(sourcePattern, sinkPattern);

        // Match the pattern against the plan.
        final List<SubplanMatch> matches = subplanPattern.match(plan, Operator.FIRST_EPOCH);

        // Evaluate the matches.
        Assert.assertEquals(1, matches.size());
        final SubplanMatch match = matches.get(0);
        Assert.assertEquals(source, match.getOperatorMatches().get("source").getOperator());
        Assert.assertEquals(sink, match.getOperatorMatches().get("sink").getOperator());
    }

}
