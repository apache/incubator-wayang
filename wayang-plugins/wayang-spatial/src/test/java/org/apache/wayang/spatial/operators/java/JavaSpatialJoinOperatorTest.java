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

package org.apache.wayang.spatial.operators.java;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.spatial.SpatialPredicate;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.profiling.NoInstrumentationStrategy;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spatial.data.WayangGeometry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for {@link JavaSpatialJoinOperator}.
 */
class JavaSpatialJoinOperatorTest {

    private static Configuration configuration;
    private static Job job;

    @BeforeAll
    static void init() {
        configuration = new Configuration();
        job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        DefaultOptimizationContext optimizationContext = new DefaultOptimizationContext(job);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));
        when(job.getOptimizationContext()).thenReturn(optimizationContext);
    }

    private static JavaExecutor createExecutor() {
        return new JavaExecutor(JavaPlatform.getInstance(), job);
    }

    private static OptimizationContext.OperatorContext createOperatorContext(Operator operator) {
        OptimizationContext optimizationContext = job.getOptimizationContext();
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.addOneTimeOperator(operator);
        for (int i = 0; i < operator.getNumInputs(); i++) {
            operatorContext.setInputCardinality(i, new CardinalityEstimate(100, 10000, 0.1));
        }
        for (int i = 0; i < operator.getNumOutputs(); i++) {
            operatorContext.setOutputCardinality(i, new CardinalityEstimate(100, 10000, 0.1));
        }
        return operatorContext;
    }

    private static StreamChannel.Instance createStreamChannelInstance() {
        return (StreamChannel.Instance) StreamChannel.DESCRIPTOR
                .createChannel(null, configuration)
                .createInstance(mock(JavaExecutor.class), null, -1);
    }

    private static StreamChannel.Instance createStreamChannelInstance(Stream<?> stream) {
        StreamChannel.Instance instance = createStreamChannelInstance();
        instance.accept(stream);
        return instance;
    }

    @Test
    void testIntersectsJoin() {
        // Left: 3 points — two in box1, one in box2
        List<WayangGeometry> left = Arrays.asList(
                new WayangGeometry("POINT (0.5 0.5)"),
                new WayangGeometry("POINT (0.5 0.8)"),
                new WayangGeometry("POINT (5.5 5.5)")
        );

        // Right: 2 non-overlapping boxes
        List<WayangGeometry> right = Arrays.asList(
                new WayangGeometry("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"),
                new WayangGeometry("POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))")
        );

        JavaSpatialJoinOperator<WayangGeometry, WayangGeometry> joinOp = new JavaSpatialJoinOperator<>(
                w -> w,
                w -> w,
                WayangGeometry.class,
                WayangGeometry.class,
                SpatialPredicate.INTERSECTS
        );

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{
                createStreamChannelInstance(left.stream()),
                createStreamChannelInstance(right.stream())
        };
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        joinOp.evaluate(inputs, outputs, createExecutor(), createOperatorContext(joinOp));

        List<Tuple2<WayangGeometry, WayangGeometry>> result =
                outputs[0].<Tuple2<WayangGeometry, WayangGeometry>>provideStream().collect(Collectors.toList());
        assertEquals(3, result.size());
    }

    @Test
    void testJoinNoMatches() {
        // Left: points far from right boxes
        List<WayangGeometry> left = Arrays.asList(
                new WayangGeometry("POINT (100 100)"),
                new WayangGeometry("POINT (200 200)")
        );

        List<WayangGeometry> right = Arrays.asList(
                new WayangGeometry("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"),
                new WayangGeometry("POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))")
        );

        JavaSpatialJoinOperator<WayangGeometry, WayangGeometry> joinOp = new JavaSpatialJoinOperator<>(
                w -> w,
                w -> w,
                WayangGeometry.class,
                WayangGeometry.class,
                SpatialPredicate.INTERSECTS
        );

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{
                createStreamChannelInstance(left.stream()),
                createStreamChannelInstance(right.stream())
        };
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        joinOp.evaluate(inputs, outputs, createExecutor(), createOperatorContext(joinOp));

        List<Tuple2<WayangGeometry, WayangGeometry>> result =
                outputs[0].<Tuple2<WayangGeometry, WayangGeometry>>provideStream().collect(Collectors.toList());
        assertEquals(0, result.size());
    }

    @Test
    void testJoinWithStringKeyExtractor() {
        // Input type is String (WKT), key extractors parse via WayangGeometry.fromStringInput
        List<String> left = Arrays.asList(
                "POINT (0.5 0.5)",
                "POINT (5.5 5.5)"
        );

        List<String> right = Arrays.asList(
                "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
                "POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))"
        );

        JavaSpatialJoinOperator<String, String> joinOp = new JavaSpatialJoinOperator<>(
                WayangGeometry::fromStringInput,
                WayangGeometry::fromStringInput,
                String.class,
                String.class,
                SpatialPredicate.INTERSECTS
        );

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{
                createStreamChannelInstance(left.stream()),
                createStreamChannelInstance(right.stream())
        };
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        joinOp.evaluate(inputs, outputs, createExecutor(), createOperatorContext(joinOp));

        List<Tuple2<String, String>> result =
                outputs[0].<Tuple2<String, String>>provideStream().collect(Collectors.toList());
        assertEquals(2, result.size());
    }
}
