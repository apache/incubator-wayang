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

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for {@link FlinkUnionAllOperator}.
 */
public class FlinkUnionAllOperatorTest extends FlinkOperatorTestBase {

    @Test
    public void testExecution() throws Exception {
        // Prepare test data.
        DataSetChannel.Instance input0 = this.createDataSetChannelInstance(Arrays.asList(6, 0, 1, 1, 5, 2));
        DataSetChannel.Instance input1 = this.createDataSetChannelInstance(Arrays.asList(1, 1, 9));
        DataSetChannel.Instance output = this.createDataSetChannelInstance();

        // Build the UnionAll operator.
        FlinkUnionAllOperator<Integer> unionAllOperator =
                new FlinkUnionAllOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class)
                );

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{input0, input1};
        final ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(unionAllOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = output.<Integer>provideDataSet().collect();
        // final List<Integer> result = ((DataSetChannel.Instance) outputs[0]).<Integer>provideDataSet().collect();
        Assert.assertEquals(9, result.size());
        Assert.assertEquals(Arrays.asList(6, 0, 1, 1, 5, 2, 1, 1, 9), result);
    }
}
