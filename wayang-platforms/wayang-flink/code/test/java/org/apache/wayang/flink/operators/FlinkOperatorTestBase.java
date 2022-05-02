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

package org.apache.wayang.flink.operators;

import org.junit.Before;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.profiling.FullInstrumentationStrategy;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.apache.wayang.flink.execution.FlinkExecutor;
import org.apache.wayang.flink.platform.FlinkPlatform;
import org.apache.wayang.flink.test.ChannelFactory;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.flink.operators.FlinkExecutionOperator;

import java.util.Collection;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test base for {@link FlinkExecutionOperator} tests.
 */
public class FlinkOperatorTestBase {

    protected Configuration configuration;

    protected FlinkExecutor flinkExecutor;

    @Before
    public void setUp(){
        this.configuration = new Configuration();
        if(flinkExecutor == null)
            this.flinkExecutor = (FlinkExecutor) FlinkPlatform.getInstance().getExecutorFactory().create(this.mockJob());
    }

    Job mockJob() {
        final Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(this.configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new FullInstrumentationStrategy()));
        return job;
    }

    protected OptimizationContext.OperatorContext createOperatorContext(Operator operator) {
        OptimizationContext optimizationContext = new DefaultOptimizationContext(mockJob());
        return optimizationContext.addOneTimeOperator(operator);
    }


    protected void evaluate(FlinkExecutionOperator operator,
                            ChannelInstance[] inputs,
                            ChannelInstance[] outputs) throws Exception {
        operator.evaluate(inputs, outputs, this.flinkExecutor, this.createOperatorContext(operator));
    }


    DataSetChannel.Instance createDataSetChannelInstance() {
        return ChannelFactory.createDataSetChannelInstance(this.configuration);
    }

    DataSetChannel.Instance createDataSetChannelInstance(Collection<?> collection) {
        return ChannelFactory.createDataSetChannelInstance(collection, this.flinkExecutor, this.configuration);
    }

    protected CollectionChannel.Instance createCollectionChannelInstance() {
        return ChannelFactory.createCollectionChannelInstance(this.configuration);
    }

    protected CollectionChannel.Instance createCollectionChannelInstance(Collection<?> collection) {
        return ChannelFactory.createCollectionChannelInstance(collection, this.configuration);
    }

    public ExecutionEnvironment getEnv() {
        return this.flinkExecutor.fee;
    }

}
