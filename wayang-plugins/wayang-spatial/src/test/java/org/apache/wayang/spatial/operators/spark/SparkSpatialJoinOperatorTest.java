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

package org.apache.wayang.spatial.operators.spark;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.spatial.SpatialPredicate;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.profiling.FullInstrumentationStrategy;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.apache.wayang.spark.operators.SparkExecutionOperator;
import org.apache.wayang.spark.platform.SparkPlatform;
import org.apache.wayang.spatial.data.WayangGeometry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Test suite for {@link SparkSpatialJoinOperator}.
 */
class SparkSpatialJoinOperatorTest {

    private Configuration configuration;
    private SparkExecutor sparkExecutor;
    private Job job;

    @BeforeEach
    void setUp() {
        WayangContext context = new WayangContext(new Configuration());
        this.job = context.createJob("spark-spatial-join-test", new WayangPlan());
        this.configuration = this.job.getConfiguration();
        this.ensureCrossPlatformExecutor();
        this.sparkExecutor = (SparkExecutor) SparkPlatform.getInstance().getExecutorFactory().create(this.job);
    }

    private void ensureCrossPlatformExecutor() {
        try {
            Field field = Job.class.getDeclaredField("crossPlatformExecutor");
            field.setAccessible(true);
            if (field.get(this.job) == null) {
                CrossPlatformExecutor executor = new CrossPlatformExecutor(this.job, new FullInstrumentationStrategy());
                field.set(this.job, executor);
            }
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to initialize CrossPlatformExecutor for tests.", e);
        }
    }

    private OptimizationContext.OperatorContext createOperatorContext(Operator operator) {
        OptimizationContext optimizationContext = new DefaultOptimizationContext(this.job);
        return optimizationContext.addOneTimeOperator(operator);
    }

    private void evaluate(SparkExecutionOperator operator,
                          ChannelInstance[] inputs,
                          ChannelInstance[] outputs) {
        operator.evaluate(inputs, outputs, this.sparkExecutor, this.createOperatorContext(operator));
    }

    private RddChannel.Instance createRddChannelInstance() {
        return (RddChannel.Instance) RddChannel.UNCACHED_DESCRIPTOR
                .createChannel(null, this.configuration)
                .createInstance(mock(SparkExecutor.class), null, -1);
    }

    private RddChannel.Instance createRddChannelInstance(Collection<?> collection) {
        RddChannel.Instance instance = createRddChannelInstance();
        instance.accept(this.sparkExecutor.sc.parallelize(WayangCollections.asList(collection)), this.sparkExecutor);
        return instance;
    }

    @Test
    void testIntersectsJoin() {
        // Left: 3 polygons, two overlap with the right polygon, one doesn't
        List<WayangGeometry> left = Arrays.asList(
                WayangGeometry.fromStringInput("POLYGON ((0 0, 0 0.1, 0.1 0.1, 0.1 0, 0 0))"),
                WayangGeometry.fromStringInput("POLYGON ((0.2 0.2, 0.2 0.3, 0.3 0.3, 0.3 0.2, 0.2 0.2))"),
                WayangGeometry.fromStringInput("POLYGON ((0.4 0, 0.4 0.5, 0.5 0.5, 0.5 0.4, 0.4 0))")
        );

        // Right: 1 polygon that overlaps with polygon #2 from left
        List<WayangGeometry> right = Arrays.asList(
                WayangGeometry.fromStringInput("POLYGON ((0.9 0.9, 0.9 1, 1 1, 1 0.9, 0.9 0.9))"),
                WayangGeometry.fromStringInput("POLYGON ((0.2 0.2, 0.2 0.3, 0.3 0.3, 0.3 0.2, 0.2 0.2))")
        );

        SparkSpatialJoinOperator<WayangGeometry, WayangGeometry> joinOp = new SparkSpatialJoinOperator<>(
                w -> w,
                w -> w,
                WayangGeometry.class,
                WayangGeometry.class,
                SpatialPredicate.INTERSECTS
        );

        RddChannel.Instance leftChannel = this.createRddChannelInstance(left);
        RddChannel.Instance rightChannel = this.createRddChannelInstance(right);
        RddChannel.Instance outputChannel = this.createRddChannelInstance();

        this.evaluate(joinOp,
                new ChannelInstance[]{leftChannel, rightChannel},
                new ChannelInstance[]{outputChannel});

        List<Tuple2<WayangGeometry, WayangGeometry>> result =
                outputChannel.<Tuple2<WayangGeometry, WayangGeometry>>provideRdd().collect();
        assertEquals(1, result.size());
    }

    @Test
    void testJoinNoMatches() {
        List<WayangGeometry> left = Arrays.asList(
                WayangGeometry.fromStringInput("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"),
                WayangGeometry.fromStringInput("POLYGON ((2 2, 2 3, 3 3, 3 2, 2 2))")
        );

        List<WayangGeometry> right = Arrays.asList(
                WayangGeometry.fromStringInput("POLYGON ((10 10, 10 11, 11 11, 11 10, 10 10))"),
                WayangGeometry.fromStringInput("POLYGON ((20 20, 20 21, 21 21, 21 20, 20 20))")
        );

        SparkSpatialJoinOperator<WayangGeometry, WayangGeometry> joinOp = new SparkSpatialJoinOperator<>(
                w -> w,
                w -> w,
                WayangGeometry.class,
                WayangGeometry.class,
                SpatialPredicate.INTERSECTS
        );

        RddChannel.Instance leftChannel = this.createRddChannelInstance(left);
        RddChannel.Instance rightChannel = this.createRddChannelInstance(right);
        RddChannel.Instance outputChannel = this.createRddChannelInstance();

        this.evaluate(joinOp,
                new ChannelInstance[]{leftChannel, rightChannel},
                new ChannelInstance[]{outputChannel});

        List<Tuple2<WayangGeometry, WayangGeometry>> result =
                outputChannel.<Tuple2<WayangGeometry, WayangGeometry>>provideRdd().collect();
        assertEquals(0, result.size());
    }
}
