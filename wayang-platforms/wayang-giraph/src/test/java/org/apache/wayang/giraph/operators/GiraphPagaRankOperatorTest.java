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

package org.apache.wayang.giraph.operators;

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
import org.apache.wayang.giraph.Giraph;
import org.apache.wayang.giraph.execution.GiraphExecutor;
import org.apache.wayang.giraph.platform.GiraphPlatform;
import org.apache.wayang.java.channels.StreamChannel;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test For GiraphPageRank
 */
public class GiraphPagaRankOperatorTest {

    private static GiraphExecutor giraphExecutor;

    @Before
    public void setUp() {
        giraphExecutor = mock(GiraphExecutor.class);
    }

    //TODO Validate the mock of GiraphExecutor @Test
    public void testExecution() throws IOException {
        // Ensure that the GraphChiPlatform is initialized.
        GiraphPlatform.getInstance();
        final Configuration configuration = new Configuration();
        Giraph.plugin().configure(configuration);
        final GiraphPageRankOperator giraphPageRankOperator = new GiraphPageRankOperator(20);

        final Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new FullInstrumentationStrategy()));

        final ExecutionOperator outputOperator = mock(ExecutionOperator.class);
        when(outputOperator.getNumOutputs()).thenReturn(1);
        FileChannel.Instance inputChannelInstance =
                (FileChannel.Instance) new FileChannel(FileChannel.HDFS_TSV_DESCRIPTOR)
                        .createInstance(giraphExecutor, null, -1);
        inputChannelInstance.addPath(this.getClass().getResource("/test.edgelist.input").toString());
        inputChannelInstance.getLineage().collectAndMark();

        final ExecutionOperator inputOperator = mock(ExecutionOperator.class);
        when(inputOperator.getNumOutputs()).thenReturn(1);
        StreamChannel.Instance outputFileChannelInstance =
                (StreamChannel.Instance) StreamChannel.DESCRIPTOR
                        .createChannel(giraphPageRankOperator.getOutput(), configuration)
                        .createInstance(giraphExecutor, null, -1);

        final DefaultOptimizationContext optimizationContext = new DefaultOptimizationContext(job);
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.addOneTimeOperator(giraphPageRankOperator);

        giraphPageRankOperator.execute(
                new ChannelInstance[]{inputChannelInstance},
                new ChannelInstance[]{outputFileChannelInstance},
                giraphExecutor,
                operatorContext
        );
    }
}
