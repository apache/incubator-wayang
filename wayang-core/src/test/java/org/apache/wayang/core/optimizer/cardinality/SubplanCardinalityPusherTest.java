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
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.plan.wayangplan.Subplan;
import org.apache.wayang.core.plan.wayangplan.test.TestJoin;
import org.apache.wayang.core.plan.wayangplan.test.TestMapOperator;
import org.apache.wayang.core.plan.wayangplan.test.TestSource;
import org.apache.wayang.core.test.MockFactory;
import org.apache.wayang.core.types.DataSetType;

/**
 * Test suite for {@link SubplanCardinalityPusher}.
 */
public class SubplanCardinalityPusherTest {

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
    public void testSimpleSubplan() {
        TestMapOperator<String, String> op1 = new TestMapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefault(String.class)
        );
        TestMapOperator<String, String> op2 = new TestMapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefault(String.class)
        );

        op1.connectTo(0, op2, 0);

        Subplan subplan = (Subplan) Subplan.wrap(op1, op2);
        OptimizationContext optimizationContext = new DefaultOptimizationContext(this.job, subplan);
        final OptimizationContext.OperatorContext subplanCtx = optimizationContext.getOperatorContext(subplan);
        final CardinalityEstimate inputCardinality = new CardinalityEstimate(123, 321, 0.123d);
        subplanCtx.setInputCardinality(0, inputCardinality);
        subplan.propagateInputCardinality(0, subplanCtx);

        final CardinalityPusher pusher = SubplanCardinalityPusher.createFor(subplan, this.configuration);
        pusher.push(subplanCtx, this.configuration);

        Assert.assertEquals(inputCardinality, subplanCtx.getOutputCardinality(0));
    }

    @Test
    public void testSourceSubplan() {
        TestSource<String> source = new TestSource<>(DataSetType.createDefault(String.class));
        final CardinalityEstimate sourceCardinality = new CardinalityEstimate(123, 321, 0.123d);
        source.setCardinalityEstimators((optimizationContext, inputEstimates) -> sourceCardinality);

        TestMapOperator<String, String> op = new TestMapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefault(String.class)
        );

        source.connectTo(0, op, 0);

        Subplan subplan = (Subplan) Subplan.wrap(source, op);
        OptimizationContext optimizationContext = new DefaultOptimizationContext(this.job, subplan);
        final OptimizationContext.OperatorContext subplanCtx = optimizationContext.getOperatorContext(subplan);

        final CardinalityPusher pusher = SubplanCardinalityPusher.createFor(subplan, this.configuration);
        pusher.push(subplanCtx, this.configuration);

        Assert.assertEquals(sourceCardinality, subplanCtx.getOutputCardinality(0));
    }


    @Test
    public void testDAGShapedSubplan() {
        // _/-\_
        //  \ /
        final DataSetType<String> stringDataSetType = DataSetType.createDefault(String.class);
        TestMapOperator<String, String> map1 = new TestMapOperator<>(stringDataSetType, stringDataSetType);
        map1.setName("map1");
        TestMapOperator<String, String> map2 = new TestMapOperator<>(stringDataSetType, stringDataSetType);
        map2.setName("map2");
        TestMapOperator<String, String> map3 = new TestMapOperator<>(stringDataSetType, stringDataSetType);
        map3.setName("map3");
        TestJoin<String, String, String> join1 = new TestJoin<>(stringDataSetType, stringDataSetType, stringDataSetType);
        join1.setName("join1");
        TestMapOperator<String, String> map4 = new TestMapOperator<>(stringDataSetType, stringDataSetType);
        map4.setName("map4");

        map1.connectTo(0, map2, 0);
        map1.connectTo(0, map3, 0);
        map2.connectTo(0, join1, 0);
        map3.connectTo(0, join1, 1);
        join1.connectTo(0, map4, 0);

        Subplan subplan = (Subplan) Subplan.wrap(map1, map4);
        OptimizationContext optimizationContext = new DefaultOptimizationContext(this.job, subplan);
        final OptimizationContext.OperatorContext subplanCtx = optimizationContext.getOperatorContext(subplan);
        final CardinalityEstimate inputCardinality = new CardinalityEstimate(10, 100, 0.9d);
        subplanCtx.setInputCardinality(0, inputCardinality);
        subplan.propagateInputCardinality(0, subplanCtx);

        final CardinalityPusher pusher = SubplanCardinalityPusher.createFor(subplan, this.configuration);
        pusher.push(subplanCtx, this.configuration);

        final CardinalityEstimate outputCardinality = subplanCtx.getOutputCardinality(0);
        final CardinalityEstimate expectedCardinality = new CardinalityEstimate(10 * 10, 100 * 100, 0.9d * 0.7d);
        Assert.assertEquals(expectedCardinality, outputCardinality);
    }

}
