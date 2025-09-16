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

import org.apache.wayang.core.plan.wayangplan.test.TestLoopHead;
import org.apache.wayang.core.plan.wayangplan.test.TestMapOperator;
import org.apache.wayang.core.plan.wayangplan.test.TestSink;
import org.apache.wayang.core.plan.wayangplan.test.TestSource;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Test suite for the {@link LoopIsolator}.
 */
class LoopIsolatorTest {

    @Test
    void testWithSingleLoop() {
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
        assertSame(LoopSubplan.class, allegedLoopSubplan.getClass());
        assertSame(source, allegedLoopSubplan.getEffectiveOccupant(0).getOwner());

        // Check the Subplan.
        LoopSubplan loopSubplan = (LoopSubplan) allegedLoopSubplan;
        assertEquals(1, loopSubplan.getNumOutputs());
        assertEquals(1, loopSubplan.getNumInputs());
        assertSame(loopHead.getOutput("finalOutput"), loopSubplan.traceOutput(loopSubplan.getOutput(0)));
        List<InputSlot<?>> innerInputs = new ArrayList<>(loopSubplan.followInput(loopSubplan.getInput(0)));
        assertEquals(1, innerInputs.size());
        assertSame(loopHead.getInput("initialInput"), innerInputs.get(0));
        assertSame(loopSubplan, loopHead.getParent());
        assertSame(loopSubplan, inLoopMap.getParent());
    }

    @Test
    void testWithSingleLoopWithConstantInput() {
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
        assertSame(LoopSubplan.class, allegedLoopSubplan.getClass());
        assertSame(mainSource, allegedLoopSubplan.getEffectiveOccupant(0).getOwner());

        // Check the Subplan.
        LoopSubplan loopSubplan = (LoopSubplan) allegedLoopSubplan;
        assertEquals(1, loopSubplan.getNumOutputs());
        assertEquals(2, loopSubplan.getNumInputs());
        assertSame(loopHead.getOutput("finalOutput"), loopSubplan.traceOutput(loopSubplan.getOutput(0)));
        List<InputSlot<?>> innerInputs = new ArrayList<>(loopSubplan.followInput(loopSubplan.getInput(0)));
        assertEquals(1, innerInputs.size());
        assertSame(loopHead.getInput("initialInput"), innerInputs.get(0));
        assertSame(loopSubplan, loopHead.getParent());
        assertSame(loopSubplan, inLoopMap.getParent());
    }

    @Test
    void testNestedLoops() {
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
        assertSame(LoopSubplan.class, allegedOuterLoopSubplan.getClass());
        assertSame(mainSource, allegedOuterLoopSubplan.getEffectiveOccupant(0).getOwner());

        // Check the outer Subplan.
        LoopSubplan outerSubplan = (LoopSubplan) allegedOuterLoopSubplan;
        assertEquals(1, outerSubplan.getNumOutputs());
        assertEquals(1, outerSubplan.getNumInputs());
        assertSame(outerLoopHead.getOutput("finalOutput"), outerSubplan.traceOutput(outerSubplan.getOutput(0)));
        List<InputSlot<?>> innerInputs = new ArrayList<>(outerSubplan.followInput(outerSubplan.getInput(0)));
        assertEquals(1, innerInputs.size());
        assertSame(outerLoopHead.getInput("initialInput"), innerInputs.get(0));
        assertSame(outerSubplan, outerLoopHead.getParent());
        assertSame(outerSubplan, inOuterLoopMap.getParent());
        final Operator allegedInnerLoopSubplan = outerLoopHead.getEffectiveOccupant(outerLoopHead.getInput("loopInput")).getOwner();
        assertSame(LoopSubplan.class, allegedInnerLoopSubplan.getClass());

        // Check the inner Subplan.
        LoopSubplan innerSubplan = (LoopSubplan) allegedInnerLoopSubplan;
        assertEquals(1, innerSubplan.getNumOutputs());
        assertEquals(1, innerSubplan.getNumInputs());
        assertSame(innerLoopHead.getOutput("finalOutput"), innerSubplan.traceOutput(innerSubplan.getOutput(0)));
        innerInputs = new ArrayList<>(innerSubplan.followInput(innerSubplan.getInput(0)));
        assertEquals(1, innerInputs.size());
        assertSame(innerLoopHead.getInput("initialInput"), innerInputs.get(0));
        assertSame(innerSubplan, innerLoopHead.getParent());
        assertSame(innerSubplan, inInnerLoopMap.getParent());

    }
}
