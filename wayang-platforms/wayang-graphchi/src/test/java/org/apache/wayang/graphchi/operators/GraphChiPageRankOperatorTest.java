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

package org.apache.wayang.graphchi.operators;

import org.junit.Before;
import org.junit.Test;
import org.apache.wayang.basic.channels.FileChannel;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.profiling.FullInstrumentationStrategy;
import org.apache.wayang.graphchi.GraphChi;
import org.apache.wayang.graphchi.execution.GraphChiExecutor;
import org.apache.wayang.graphchi.platform.GraphChiPlatform;
import org.apache.wayang.graphchi.operators.GraphChiPageRankOperator;
import org.apache.wayang.java.channels.StreamChannel;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for the {@link GraphChiPageRankOperator}.
 */
public class GraphChiPageRankOperatorTest {

    private static GraphChiExecutor graphChiExecutor;

    @Before
    public void setUp() {
        graphChiExecutor = mock(GraphChiExecutor.class);
    }

    @Test
    public void testExecution() throws IOException {
        // Ensure that the GraphChiPlatform is initialized.
        GraphChiPlatform.getInstance();
        final Configuration configuration = new Configuration();
        GraphChi.plugin().configure(configuration);
        final GraphChiPageRankOperator graphChiPageRankOperator = new GraphChiPageRankOperator(20);

        final Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new FullInstrumentationStrategy()));

        final ExecutionOperator outputOperator = mock(ExecutionOperator.class);
        when(outputOperator.getNumOutputs()).thenReturn(1);
        FileChannel.Instance inputChannelInstance =
                (FileChannel.Instance) new FileChannel(FileChannel.HDFS_TSV_DESCRIPTOR)
                        .createInstance(graphChiExecutor, null, -1);
        inputChannelInstance.addPath(this.getClass().getResource("/test.edgelist").toString());
        inputChannelInstance.getLineage().collectAndMark();

        final ExecutionOperator inputOperator = mock(ExecutionOperator.class);
        when(inputOperator.getNumOutputs()).thenReturn(1);
        StreamChannel.Instance outputFileChannelInstance =
                (StreamChannel.Instance) StreamChannel.DESCRIPTOR
                        .createChannel(graphChiPageRankOperator.getOutput(), configuration)
                        .createInstance(graphChiExecutor, null, -1);

        final DefaultOptimizationContext optimizationContext = new DefaultOptimizationContext(job);
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.addOneTimeOperator(graphChiPageRankOperator);

        graphChiPageRankOperator.execute(
                new ChannelInstance[]{inputChannelInstance},
                new ChannelInstance[]{outputFileChannelInstance},
                operatorContext
        );
    }

}
