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

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.spatial.SpatialPredicate;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.profiling.NoInstrumentationStrategy;
import org.apache.wayang.core.types.DataSetType;
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
 * Test suite for {@link JavaSpatialFilterOperator}.
 */
class JavaSpatialFilterOperatorTest {

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
    void testIntersectsFilter() {
        // 4 polygons: larger than reference, overlapping, fully inside, fully outside
        List<WayangGeometry> input = Arrays.asList(
                new WayangGeometry("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))"),
                new WayangGeometry("POLYGON ((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))"),
                new WayangGeometry("POLYGON ((0.2 0.2, 0.8 0.2, 0.8 0.8, 0.2 0.8, 0.2 0.2))"),
                new WayangGeometry("POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))")
        );

        WayangGeometry reference = new WayangGeometry("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");

        JavaSpatialFilterOperator<WayangGeometry> filterOp = new JavaSpatialFilterOperator<>(
                SpatialPredicate.INTERSECTS,
                w -> w,
                DataSetType.createDefault(WayangGeometry.class),
                reference
        );

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(input.stream())};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        filterOp.evaluate(inputs, outputs, createExecutor(), createOperatorContext(filterOp));

        List<WayangGeometry> result = outputs[0].<WayangGeometry>provideStream().collect(Collectors.toList());
        assertEquals(3, result.size());
    }

    @Test
    void testWithinFilter() {
        // Same 4 polygons; only the fully-inside one is WITHIN the unit square
        List<WayangGeometry> input = Arrays.asList(
                new WayangGeometry("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))"),
                new WayangGeometry("POLYGON ((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))"),
                new WayangGeometry("POLYGON ((0.2 0.2, 0.8 0.2, 0.8 0.8, 0.2 0.8, 0.2 0.2))"),
                new WayangGeometry("POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))")
        );

        WayangGeometry reference = new WayangGeometry("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");

        JavaSpatialFilterOperator<WayangGeometry> filterOp = new JavaSpatialFilterOperator<>(
                SpatialPredicate.WITHIN,
                w -> w,
                DataSetType.createDefault(WayangGeometry.class),
                reference
        );

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(input.stream())};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        filterOp.evaluate(inputs, outputs, createExecutor(), createOperatorContext(filterOp));

        List<WayangGeometry> result = outputs[0].<WayangGeometry>provideStream().collect(Collectors.toList());
        assertEquals(1, result.size());
    }

    @Test
    void testFilterNoMatches() {
        List<WayangGeometry> input = Arrays.asList(
                new WayangGeometry("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))"),
                new WayangGeometry("POLYGON ((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))"),
                new WayangGeometry("POLYGON ((0.2 0.2, 0.8 0.2, 0.8 0.8, 0.2 0.8, 0.2 0.2))"),
                new WayangGeometry("POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))")
        );

        // Distant geometry — no intersections
        WayangGeometry reference = new WayangGeometry("POLYGON ((100 100, 101 100, 101 101, 100 101, 100 100))");

        JavaSpatialFilterOperator<WayangGeometry> filterOp = new JavaSpatialFilterOperator<>(
                SpatialPredicate.INTERSECTS,
                w -> w,
                DataSetType.createDefault(WayangGeometry.class),
                reference
        );

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(input.stream())};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        filterOp.evaluate(inputs, outputs, createExecutor(), createOperatorContext(filterOp));

        List<WayangGeometry> result = outputs[0].<WayangGeometry>provideStream().collect(Collectors.toList());
        assertEquals(0, result.size());
    }
}
