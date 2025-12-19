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

package org.apache.wayang.spark.operators;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.profiling.FullInstrumentationStrategy;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.channels.DatasetChannel;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.apache.wayang.spark.platform.SparkPlatform;
import org.apache.wayang.spark.test.ChannelFactory;
import org.apache.wayang.core.api.WayangContext;
import org.junit.jupiter.api.BeforeEach;

import java.lang.reflect.Field;
import java.util.Collection;

/**
 * Test base for {@link SparkExecutionOperator} tests.
 */
class SparkOperatorTestBase {

    protected Configuration configuration;

    protected SparkExecutor sparkExecutor;

    private Job job;

    @BeforeEach
    void setUp() {
        WayangContext context = new WayangContext(new Configuration());
        this.job = context.createJob("spark-operator-test", new WayangPlan());
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

    protected OptimizationContext.OperatorContext createOperatorContext(Operator operator) {
        OptimizationContext optimizationContext = new DefaultOptimizationContext(this.job);
        return optimizationContext.addOneTimeOperator(operator);
    }

    protected void evaluate(SparkExecutionOperator operator,
                            ChannelInstance[] inputs,
                            ChannelInstance[] outputs) {
        operator.evaluate(inputs, outputs, this.sparkExecutor, this.createOperatorContext(operator));
    }

    RddChannel.Instance createRddChannelInstance() {
        return ChannelFactory.createRddChannelInstance(this.configuration);
    }

    RddChannel.Instance createRddChannelInstance(Collection<?> collection) {
        return ChannelFactory.createRddChannelInstance(collection, this.sparkExecutor, this.configuration);
    }

    DatasetChannel.Instance createDatasetChannelInstance() {
        return ChannelFactory.createDatasetChannelInstance(this.configuration);
    }

    DatasetChannel.Instance createDatasetChannelInstance(Dataset<Row> dataset) {
        return ChannelFactory.createDatasetChannelInstance(dataset, this.sparkExecutor, this.configuration);
    }

    protected CollectionChannel.Instance createCollectionChannelInstance() {
        return ChannelFactory.createCollectionChannelInstance(this.configuration);
    }

    protected CollectionChannel.Instance createCollectionChannelInstance(Collection<?> collection) {
        return ChannelFactory.createCollectionChannelInstance(collection, this.configuration);
    }

    public JavaSparkContext getSC() {
        return this.sparkExecutor.sc;
    }

}
