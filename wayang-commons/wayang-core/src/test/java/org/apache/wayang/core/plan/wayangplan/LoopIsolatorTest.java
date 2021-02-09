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
import org.apache.wayang.core.plan.wayangplan.test.TestLoopHead;
import org.apache.wayang.core.plan.wayangplan.test.TestMapOperator;
import org.apache.wayang.core.plan.wayangplan.test.TestSink;
import org.apache.wayang.core.plan.wayangplan.test.TestSource;

import java.util.ArrayList;
import java.util.List;

/**
 * Test suite for the {@link LoopIsolator}.
 */
public class LoopIsolatorTest {

    @Test
    public void testWithSingleLoop() {
        TestSource<Integer> source = new TestSource<>(Integer.class);

        TestLoopHead<Integer> loopHead = new TestLoopHead<>(Integer.class);
        source.connectTo("out", loopHead, "initialInput");

        TestMapOperator<Integer, Integer> inLoopMap = new TestMapOperator<>(Integer.class, Integer.class);
        loopHead.connectTo("loopOutput", inLoopMap, "in");
        inLoopMap.connectTo("out", loopHead, "loopInput");

        TestSink<Integer> sink = new TestSink<>(Integer.class);
        loopHead.connectTo("finalOutput", sink, "in");

        WayangPlan wayangPlan = new WayangPlan(sink);

        LoopIsolator.isolateLoops(wayangPlan);

        // Check the top-level plan.
        final Operator allegedLoopSubplan = sink.getEffectiveOccupant(0).getOwner();
        Assert.assertSame(LoopSubplan.class, allegedLoopSubplan.getClass());
        Assert.assertSame(source, allegedLoopSubplan.getEffectiveOccupant(0).getOwner());

        // Check the Subplan.
        LoopSubplan loopSubplan = (LoopSubplan) allegedLoopSubplan;
        Assert.assertEquals(1, loopSubplan.getNumOutputs());
        Assert.assertEquals(1, loopSubplan.getNumInputs());
        Assert.assertSame(loopHead.getOutput("finalOutput"), loopSubplan.traceOutput(loopSubplan.getOutput(0)));
        List<InputSlot<?>> innerInputs = new ArrayList<>(loopSubplan.followInput(loopSubplan.getInput(0)));
        Assert.assertEquals(1, innerInputs.size());
        Assert.assertSame(loopHead.getInput("initialInput"), innerInputs.get(0));
        Assert.assertSame(loopSubplan, loopHead.getParent());
        Assert.assertSame(loopSubplan, inLoopMap.getParent());
    }

    @Test
    public void testWithSingleLoopWithConstantInput() {
        TestSource<Integer> mainSource = new TestSource<>(Integer.class);
        TestSource<Integer> additionalSource = new TestSource<>(Integer.class);

        TestLoopHead<Integer> loopHead = new TestLoopHead<>(Integer.class);
        mainSource.connectTo("out", loopHead, "initialInput");

        TestMapOperator<Integer, Integer> inLoopMap = new TestMapOperator<>(Integer.class, Integer.class);
        loopHead.broadcastTo("loopOutput", inLoopMap, "broadcast");
        additionalSource.connectTo("out", inLoopMap, "in");
        inLoopMap.connectTo("out", loopHead, "loopInput");

        TestSink<Integer> sink = new TestSink<>(Integer.class);
        loopHead.connectTo("finalOutput", sink, "in");

        WayangPlan wayangPlan = new WayangPlan(sink);

        LoopIsolator.isolateLoops(wayangPlan);

        // Check the top-level plan.
        final Operator allegedLoopSubplan = sink.getEffectiveOccupant(0).getOwner();
        Assert.assertSame(LoopSubplan.class, allegedLoopSubplan.getClass());
        Assert.assertSame(mainSource, allegedLoopSubplan.getEffectiveOccupant(0).getOwner());

        // Check the Subplan.
        LoopSubplan loopSubplan = (LoopSubplan) allegedLoopSubplan;
        Assert.assertEquals(1, loopSubplan.getNumOutputs());
        Assert.assertEquals(2, loopSubplan.getNumInputs());
        Assert.assertSame(loopHead.getOutput("finalOutput"), loopSubplan.traceOutput(loopSubplan.getOutput(0)));
        List<InputSlot<?>> innerInputs = new ArrayList<>(loopSubplan.followInput(loopSubplan.getInput(0)));
        Assert.assertEquals(1, innerInputs.size());
        Assert.assertSame(loopHead.getInput("initialInput"), innerInputs.get(0));
        Assert.assertSame(loopSubplan, loopHead.getParent());
        Assert.assertSame(loopSubplan, inLoopMap.getParent());
    }

    @Test
    public void testNestedLoops() {
        TestSource<Integer> mainSource = new TestSource<>(Integer.class);
        mainSource.setName("mainSource");

        TestLoopHead<Integer> outerLoopHead = new TestLoopHead<>(Integer.class);
        mainSource.connectTo("out", outerLoopHead, "initialInput");
        outerLoopHead.setName("outerLoopHead");

        TestMapOperator<Integer, Integer> inOuterLoopMap = new TestMapOperator<>(Integer.class, Integer.class);
        outerLoopHead.connectTo("loopOutput", inOuterLoopMap, "in");
        inOuterLoopMap.setName("inOuterLoopMap");

        TestLoopHead<Integer> innerLoopHead = new TestLoopHead<>(Integer.class);
        inOuterLoopMap.connectTo("out", innerLoopHead, "initialInput");
        innerLoopHead.setName("innerLoopHead");

        TestMapOperator<Integer, Integer> inInnerLoopMap = new TestMapOperator<>(Integer.class, Integer.class);
        innerLoopHead.connectTo("loopOutput", inInnerLoopMap, "in");
        inInnerLoopMap.connectTo("out", innerLoopHead, "loopInput");
        innerLoopHead.connectTo("finalOutput", outerLoopHead, "loopInput");
        inInnerLoopMap.setName("inInnerLoopMap");


        TestSink<Integer> sink = new TestSink<>(Integer.class);
        outerLoopHead.connectTo("finalOutput", sink, "in");
        sink.setName("sink");

        WayangPlan wayangPlan = new WayangPlan(sink);

        LoopIsolator.isolateLoops(wayangPlan);

        // Check the top-level plan.
        final Operator allegedOuterLoopSubplan = sink.getEffectiveOccupant(0).getOwner();
        Assert.assertSame(LoopSubplan.class, allegedOuterLoopSubplan.getClass());
        Assert.assertSame(mainSource, allegedOuterLoopSubplan.getEffectiveOccupant(0).getOwner());

        // Check the outer Subplan.
        LoopSubplan outerSubplan = (LoopSubplan) allegedOuterLoopSubplan;
        Assert.assertEquals(1, outerSubplan.getNumOutputs());
        Assert.assertEquals(1, outerSubplan.getNumInputs());
        Assert.assertSame(outerLoopHead.getOutput("finalOutput"), outerSubplan.traceOutput(outerSubplan.getOutput(0)));
        List<InputSlot<?>> innerInputs = new ArrayList<>(outerSubplan.followInput(outerSubplan.getInput(0)));
        Assert.assertEquals(1, innerInputs.size());
        Assert.assertSame(outerLoopHead.getInput("initialInput"), innerInputs.get(0));
        Assert.assertSame(outerSubplan, outerLoopHead.getParent());
        Assert.assertSame(outerSubplan, inOuterLoopMap.getParent());
        final Operator allegedInnerLoopSubplan = outerLoopHead.getEffectiveOccupant(outerLoopHead.getInput("loopInput")).getOwner();
        Assert.assertSame(LoopSubplan.class, allegedInnerLoopSubplan.getClass());

        // Check the inner Subplan.
        LoopSubplan innerSubplan = (LoopSubplan) allegedInnerLoopSubplan;
        Assert.assertEquals(1, innerSubplan.getNumOutputs());
        Assert.assertEquals(1, innerSubplan.getNumInputs());
        Assert.assertSame(innerLoopHead.getOutput("finalOutput"), innerSubplan.traceOutput(innerSubplan.getOutput(0)));
        innerInputs = new ArrayList<>(innerSubplan.followInput(innerSubplan.getInput(0)));
        Assert.assertEquals(1, innerInputs.size());
        Assert.assertSame(innerLoopHead.getInput("initialInput"), innerInputs.get(0));
        Assert.assertSame(innerSubplan, innerLoopHead.getParent());
        Assert.assertSame(innerSubplan, inInnerLoopMap.getParent());

    }
}
