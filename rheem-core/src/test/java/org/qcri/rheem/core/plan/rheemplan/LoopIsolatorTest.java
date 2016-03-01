package org.qcri.rheem.core.plan.rheemplan;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.plan.rheemplan.test.TestLoopHead;
import org.qcri.rheem.core.plan.rheemplan.test.TestMapOperator;
import org.qcri.rheem.core.plan.rheemplan.test.TestSink;
import org.qcri.rheem.core.plan.rheemplan.test.TestSource;

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
        source.connectTo("output", loopHead, "initialInput");

        TestMapOperator<Integer, Integer> inLoopMap = new TestMapOperator<>(Integer.class, Integer.class);
        loopHead.connectTo("loopOutput", inLoopMap, "input");
        inLoopMap.connectTo("output", loopHead, "loopInput");

        TestSink<Integer> sink = new TestSink<>(Integer.class);
        loopHead.connectTo("finalOutput", sink, "input");

        RheemPlan rheemPlan = new RheemPlan(sink);

        LoopIsolator.isolateLoops(rheemPlan);

        // Check the top-level plan.
        final Operator allegedLoopSubplan = sink.getInputOperator(0);
        Assert.assertSame(LoopSubplan.class, allegedLoopSubplan.getClass());
        Assert.assertSame(source, allegedLoopSubplan.getInputOperator(0));

        // Check the Subplan.
        LoopSubplan loopSubplan = (LoopSubplan) allegedLoopSubplan;
        Assert.assertTrue(loopSubplan.isWrapLoop());
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
        mainSource.connectTo("output", loopHead, "initialInput");

        TestMapOperator<Integer, Integer> inLoopMap = new TestMapOperator<>(Integer.class, Integer.class);
        loopHead.broadcastTo("loopOutput", inLoopMap, "broadcast");
        additionalSource.connectTo("output", inLoopMap, "input");
        inLoopMap.connectTo("output", loopHead, "loopInput");

        TestSink<Integer> sink = new TestSink<>(Integer.class);
        loopHead.connectTo("finalOutput", sink, "input");

        RheemPlan rheemPlan = new RheemPlan(sink);

        LoopIsolator.isolateLoops(rheemPlan);

        // Check the top-level plan.
        final Operator allegedLoopSubplan = sink.getInputOperator(0);
        Assert.assertSame(LoopSubplan.class, allegedLoopSubplan.getClass());
        Assert.assertSame(mainSource, allegedLoopSubplan.getInputOperator(0));

        // Check the Subplan.
        LoopSubplan loopSubplan = (LoopSubplan) allegedLoopSubplan;
        Assert.assertTrue(loopSubplan.isWrapLoop());
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

        TestLoopHead<Integer> outerLoopHead = new TestLoopHead<>(Integer.class);
        mainSource.connectTo("output", outerLoopHead, "initialInput");

        TestMapOperator<Integer, Integer> inOuterLoopMap = new TestMapOperator<>(Integer.class, Integer.class);
        outerLoopHead.connectTo("loopOutput", inOuterLoopMap, "input");

        TestLoopHead<Integer> innerLoopHead = new TestLoopHead<>(Integer.class);
        inOuterLoopMap.connectTo("output", innerLoopHead, "initialInput");

        TestMapOperator<Integer, Integer> inInnerLoopMap = new TestMapOperator<>(Integer.class, Integer.class);
        innerLoopHead.connectTo("loopOutput", inInnerLoopMap, "input");
        inInnerLoopMap.connectTo("output", innerLoopHead, "loopInput");
        innerLoopHead.connectTo("finalOutput", outerLoopHead, "loopInput");

        TestSink<Integer> sink = new TestSink<>(Integer.class);
        outerLoopHead.connectTo("finalOutput", sink, "input");

        RheemPlan rheemPlan = new RheemPlan(sink);

        LoopIsolator.isolateLoops(rheemPlan);

        // Check the top-level plan.
        final Operator allegedOuterLoopSubplan = sink.getInputOperator(0);
        Assert.assertSame(LoopSubplan.class, allegedOuterLoopSubplan.getClass());
        Assert.assertSame(mainSource, allegedOuterLoopSubplan.getInputOperator(0));

        // Check the outer Subplan.
        LoopSubplan outerSubplan = (LoopSubplan) allegedOuterLoopSubplan;
        Assert.assertTrue(outerSubplan.isWrapLoop());
        Assert.assertEquals(1, outerSubplan.getNumOutputs());
        Assert.assertEquals(1, outerSubplan.getNumInputs());
        Assert.assertSame(outerLoopHead.getOutput("finalOutput"), outerSubplan.traceOutput(outerSubplan.getOutput(0)));
        List<InputSlot<?>> innerInputs = new ArrayList<>(outerSubplan.followInput(outerSubplan.getInput(0)));
        Assert.assertEquals(1, innerInputs.size());
        Assert.assertSame(outerLoopHead.getInput("initialInput"), innerInputs.get(0));
        Assert.assertSame(outerSubplan, outerLoopHead.getParent());
        Assert.assertSame(outerSubplan, inOuterLoopMap.getParent());
        final Operator allegedInnerLoopSubplan = outerLoopHead.getInputOperator(outerLoopHead.getInput("loopInput"));
        Assert.assertSame(LoopSubplan.class, allegedInnerLoopSubplan.getClass());

        // Check the inner Subplan.
        LoopSubplan innerSubplan = (LoopSubplan) allegedInnerLoopSubplan;
        Assert.assertTrue(innerSubplan.isWrapLoop());
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
