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

package org.apache.wayang.core.optimizer.cardinality;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.configuration.FunctionalKeyValueProvider;
import org.apache.wayang.core.api.configuration.KeyValueProvider;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ElementaryOperator;
import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.core.plan.wayangplan.LoopIsolator;
import org.apache.wayang.core.plan.wayangplan.LoopSubplan;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.plan.wayangplan.test.TestFilterOperator;
import org.apache.wayang.core.plan.wayangplan.test.TestJoin;
import org.apache.wayang.core.plan.wayangplan.test.TestLoopHead;
import org.apache.wayang.core.plan.wayangplan.test.TestSource;
import org.apache.wayang.core.test.MockFactory;
import org.apache.wayang.core.util.WayangCollections;

/**
 * Test suite for {@link LoopSubplanCardinalityPusher}.
 */
public class LoopSubplanCardinalityPusherTest {

    private Job job;

    private Configuration configuration;

    @Before
    public void setUp() {
        this.configuration = new Configuration();
        KeyValueProvider<OutputSlot<?>, CardinalityEstimator> estimatorProvider =
                new FunctionalKeyValueProvider<>(
                        (outputSlot, requestee) -> {
                            assert outputSlot.getOwner().isElementary()
                                    : String.format("Cannot provide estimator for composite %s.", outputSlot.getOwner());
                            return ((ElementaryOperator) outputSlot.getOwner())
                                    .createCardinalityEstimator(outputSlot.getIndex(), this.configuration)
                                    .orElse(null);
                        },
                        this.configuration);
        this.configuration.setCardinalityEstimatorProvider(estimatorProvider);
        this.job = MockFactory.createJob(this.configuration);
    }

    @Test
    public void testWithSingleLoopAndSingleIteration() {

        TestLoopHead<Integer> loopHead = new TestLoopHead<>(Integer.class);
        loopHead.setNumExpectedIterations(1);

        TestFilterOperator<Integer> inLoopFilter = new TestFilterOperator<>(Integer.class);
        final double filterSelectivity = 0.7d;
        inLoopFilter.setSelectivity(filterSelectivity);
        loopHead.connectTo("loopOutput", inLoopFilter, "in");
        inLoopFilter.connectTo("out", loopHead, "loopInput");

        final LoopSubplan loop = LoopIsolator.isolate(loopHead);
        Assert.assertNotNull(loop);
        OptimizationContext optimizationContext = new DefaultOptimizationContext(this.job, loop);
        final OptimizationContext.OperatorContext loopCtx = optimizationContext.getOperatorContext(loop);
        final CardinalityEstimate inputCardinality = new CardinalityEstimate(123, 321, 0.123d);
        loopCtx.setInputCardinality(0, inputCardinality);
        loop.propagateInputCardinality(0, loopCtx);

        final CardinalityPusher pusher = new LoopSubplanCardinalityPusher(loop, this.configuration);
        pusher.push(loopCtx, this.configuration);

        final CardinalityEstimate expectedCardinality = new CardinalityEstimate(
                Math.round(inputCardinality.getLowerEstimate() * filterSelectivity),
                Math.round(inputCardinality.getUpperEstimate() * filterSelectivity),
                inputCardinality.getCorrectnessProbability()
        );
        Assert.assertEquals(expectedCardinality, loopCtx.getOutputCardinality(0));

    }

    @Test
    public void testWithSingleLoopAndManyIteration() {

        TestLoopHead<Integer> loopHead = new TestLoopHead<>(Integer.class);
        loopHead.setNumExpectedIterations(1000);

        TestFilterOperator<Integer> inLoopFilter = new TestFilterOperator<>(Integer.class);
        final double filterSelectivity = 0.7d;
        inLoopFilter.setSelectivity(filterSelectivity);
        loopHead.connectTo("loopOutput", inLoopFilter, "in");
        inLoopFilter.connectTo("out", loopHead, "loopInput");

        final LoopSubplan loop = LoopIsolator.isolate(loopHead);
        Assert.assertNotNull(loop);
        OptimizationContext optimizationContext = new DefaultOptimizationContext(this.job, loop);
        final OptimizationContext.OperatorContext loopCtx = optimizationContext.getOperatorContext(loop);
        final CardinalityEstimate inputCardinality = new CardinalityEstimate(123, 321, 0.123d);
        loopCtx.setInputCardinality(0, inputCardinality);
        loop.propagateInputCardinality(0, loopCtx);

        final CardinalityPusher pusher = new LoopSubplanCardinalityPusher(loop, this.configuration);
        pusher.push(loopCtx, this.configuration);

        final CardinalityEstimate expectedCardinality = new CardinalityEstimate(
                Math.round(inputCardinality.getLowerEstimate() * Math.pow(filterSelectivity, 1000)),
                Math.round(inputCardinality.getUpperEstimate() * Math.pow(filterSelectivity, 1000)),
                inputCardinality.getCorrectnessProbability()
        );
        Assert.assertTrue(expectedCardinality.equalsWithinDelta(loopCtx.getOutputCardinality(0), 0.0001, 1, 1));

    }

    @Test
    public void testWithSingleLoopWithConstantInput() {

        TestSource<Integer> mainSource = new TestSource<>(Integer.class);
        TestSource<Integer> sideSource = new TestSource<>(Integer.class);

        TestLoopHead<Integer> loopHead = new TestLoopHead<>(Integer.class);
        final int numIterations = 3;
        loopHead.setNumExpectedIterations(numIterations);
        mainSource.connectTo("out", loopHead, "initialInput");

        TestJoin<Integer, Integer, Integer> inLoopJoin = new TestJoin<>(Integer.class, Integer.class, Integer.class);
        loopHead.connectTo("loopOutput", inLoopJoin, "in0");
        sideSource.connectTo("out", inLoopJoin, "in1");
        inLoopJoin.connectTo("out", loopHead, "loopInput");

        final LoopSubplan loop = LoopIsolator.isolate(loopHead);
        Assert.assertNotNull(loop);

        OptimizationContext optimizationContext = new DefaultOptimizationContext(this.job, loop);
        final OptimizationContext.OperatorContext loopCtx = optimizationContext.getOperatorContext(loop);

        final CardinalityEstimate mainInputCardinality = new CardinalityEstimate(123, 321, 0.123d);
        InputSlot<?> mainLoopInput = WayangCollections.getSingle(mainSource.getOutput("out").getOccupiedSlots());
        loopCtx.setInputCardinality(mainLoopInput.getIndex(), mainInputCardinality);
        loop.propagateInputCardinality(mainLoopInput.getIndex(), loopCtx);

        final CardinalityEstimate sideInputCardinality = new CardinalityEstimate(5, 10, 0.9d);
        InputSlot<?> sideLoopInput = WayangCollections.getSingle(sideSource.getOutput("out").getOccupiedSlots());
        loopCtx.setInputCardinality(sideLoopInput.getIndex(), sideInputCardinality);
        loop.propagateInputCardinality(sideLoopInput.getIndex(), loopCtx);

        final CardinalityPusher pusher = new LoopSubplanCardinalityPusher(loop, this.configuration);
        pusher.push(loopCtx, this.configuration);

        final CardinalityEstimate expectedCardinality = new CardinalityEstimate(
                (long) Math.pow(sideInputCardinality.getLowerEstimate(), numIterations) * mainInputCardinality.getLowerEstimate(),
                (long) Math.pow(sideInputCardinality.getUpperEstimate(), numIterations) * mainInputCardinality.getUpperEstimate(),
                Math.min(mainInputCardinality.getCorrectnessProbability(), sideInputCardinality.getCorrectnessProbability())
                        * Math.pow(TestJoin.ESTIMATION_CERTAINTY, numIterations)
        );
        final CardinalityEstimate outputCardinality = loopCtx.getOutputCardinality(0);
        Assert.assertTrue(
                String.format("Expected %s, got %s.", expectedCardinality, outputCardinality),
                expectedCardinality.equalsWithinDelta(outputCardinality, 0.0001, 0, 0));
    }

    @Test
    public void testNestedLoops() {

        TestLoopHead<Integer> outerLoopHead = new TestLoopHead<>(Integer.class);
        outerLoopHead.setNumExpectedIterations(100);

        TestFilterOperator<Integer> inOuterLoopFilter = new TestFilterOperator<>(Integer.class);
        outerLoopHead.connectTo("loopOutput", inOuterLoopFilter, "in");
        inOuterLoopFilter.setSelectivity(0.9d);

        TestLoopHead<Integer> innerLoopHead = new TestLoopHead<>(Integer.class);
        inOuterLoopFilter.connectTo("out", innerLoopHead, "initialInput");
        innerLoopHead.setNumExpectedIterations(100);

        TestFilterOperator<Integer> inInnerLoopFilter = new TestFilterOperator<>(Integer.class);
        innerLoopHead.connectTo("loopOutput", inInnerLoopFilter, "in");
        inInnerLoopFilter.connectTo("out", innerLoopHead, "loopInput");
        innerLoopHead.connectTo("finalOutput", outerLoopHead, "loopInput");
        inInnerLoopFilter.setSelectivity(0.1d);

        LoopSubplan innerLoop = LoopIsolator.isolate(innerLoopHead);
        Assert.assertNotNull(innerLoop);
        LoopSubplan outerLoop = LoopIsolator.isolate(outerLoopHead);
        Assert.assertNotNull(outerLoop);

        OptimizationContext optimizationContext = new DefaultOptimizationContext(this.job, outerLoop);
        final OptimizationContext.OperatorContext loopCtx = optimizationContext.getOperatorContext(outerLoop);
        final CardinalityEstimate inputCardinality = new CardinalityEstimate(123, 321, 0.123d);
        loopCtx.setInputCardinality(0, inputCardinality);
        outerLoop.propagateInputCardinality(0, loopCtx);

        final CardinalityPusher pusher = new LoopSubplanCardinalityPusher(outerLoop, this.configuration);
        pusher.push(loopCtx, this.configuration);

        double loopSelectivity = Math.pow(
                inOuterLoopFilter.getSelectivity()
                        * Math.pow(inInnerLoopFilter.getSelectivity(), innerLoop.getNumExpectedIterations()),
                outerLoop.getNumExpectedIterations()
        );
        final CardinalityEstimate expectedCardinality = new CardinalityEstimate(
                Math.round(inputCardinality.getLowerEstimate() * loopSelectivity),
                Math.round(inputCardinality.getUpperEstimate() * loopSelectivity),
                inputCardinality.getCorrectnessProbability()
        );
        Assert.assertTrue(expectedCardinality.equalsWithinDelta(loopCtx.getOutputCardinality(0), 0.0001, 1, 1));

    }

}
