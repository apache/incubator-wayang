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

import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.junit.Assert;
import org.junit.Test;

import javax.xml.crypto.Data;
import java.util.Arrays;
import java.util.List;

/**
 * Test suite for {@link FlinkDistinctOperator}.
 */
//The test assert error expected 4, actual 1
public class FlinkDistinctOperatorTest extends FlinkOperatorTestBase{

    @Test
    public void testExecution() throws Exception {
        // Prepare test data.
        List<Integer> inputData = Arrays.asList(0, 1, 1, 6, 2, 2, 6, 6);

        // Build the distinct operator.
        FlinkDistinctOperator<Integer> distinctOperator =
                new FlinkDistinctOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class)
                );

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{this.createDataSetChannelInstance(inputData)};
        final ChannelInstance[] outputs = new ChannelInstance[]{this.createDataSetChannelInstance()};

        // Execute.
        this.evaluate(distinctOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = ((DataSetChannel.Instance) outputs[0]).<Integer>provideDataSet().collect();

        Assert.assertEquals(4, result.size());
        result.sort((a, b) -> a - b);
        Assert.assertEquals(Arrays.asList(0, 1, 2, 6), result);

    }
}
