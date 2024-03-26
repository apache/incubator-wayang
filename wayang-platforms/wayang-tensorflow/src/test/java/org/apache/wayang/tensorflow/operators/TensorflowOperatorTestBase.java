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

package org.apache.wayang.tensorflow.operators;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.profiling.FullInstrumentationStrategy;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.tensorflow.channels.TensorChannel;
import org.apache.wayang.tensorflow.execution.TensorflowExecutor;
import org.apache.wayang.tensorflow.platform.TensorflowPlatform;
import org.junit.Before;
import org.tensorflow.ndarray.NdArray;

import java.util.Collection;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test base for {@link TensorflowExecutionOperator} tests.
 */
public class TensorflowOperatorTestBase {
    protected Configuration configuration;

    protected TensorflowExecutor tensorflowExecutor;

    @Before
    public void setUp() {
        this.configuration = new Configuration();
        if(tensorflowExecutor == null)
            this.tensorflowExecutor = (TensorflowExecutor) TensorflowPlatform.getInstance().getExecutorFactory().create(this.mockJob());
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

    protected void evaluate(TensorflowExecutionOperator operator,
                            ChannelInstance[] inputs,
                            ChannelInstance[] outputs) {
        operator.evaluate(inputs, outputs, this.tensorflowExecutor, this.createOperatorContext(operator));
    }

    protected CollectionChannel.Instance createCollectionChannelInstance() {
        return (CollectionChannel.Instance) CollectionChannel.DESCRIPTOR
                .createChannel(null, configuration)
                .createInstance(tensorflowExecutor, null, -1);
    }

    protected CollectionChannel.Instance createCollectionChannelInstance(Collection<?> collection) {
        CollectionChannel.Instance instance = createCollectionChannelInstance();
        instance.accept(collection);
        return instance;
    }

    protected TensorChannel.Instance createNdArrayChannelInstance() {
        return (TensorChannel.Instance) TensorChannel.DESCRIPTOR
                .createChannel(null, configuration)
                .createInstance(tensorflowExecutor, null, -1);
    }

    protected TensorChannel.Instance createNdArrayChannelInstance(NdArray ndArray) {
        TensorChannel.Instance instance = createNdArrayChannelInstance();
        instance.accept(ndArray);
        return instance;
    }
}
