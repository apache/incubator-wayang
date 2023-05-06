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

import org.apache.wayang.core.function.MapPartitionsDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Test suite for {@link FlinkMapPartitionsOperator}.
 */
public class FlinkMapPartitionsOperatorTest extends FlinkOperatorTestBase {

    @Test
    public void testExecution() throws Exception {
        // Prepare test data.
        DataSetChannel.Instance input = this.createDataSetChannelInstance(Arrays.asList(0, 1, 1, 2, 6));
        DataSetChannel.Instance output = this.createDataSetChannelInstance();

        // Create the mapPartitions operator.
        FlinkMapPartitionsOperator<Integer, Integer> mapPartitionsOperator =
                new FlinkMapPartitionsOperator<>(
                        new MapPartitionsDescriptor<>(items -> {
                            Collection<Integer> result = new LinkedList<>();
                            for (Integer item : items) {
                                result.add(item + 1);
                            }
                            return result;
                        }, Integer.class, Integer.class),
                        DataSetType.createDefaultUnchecked(Integer.class),
                        DataSetType.createDefaultUnchecked(Integer.class)
                );

        // Set up the ChannelInstances.
        ChannelInstance[] inputs = new ChannelInstance[]{input};
        ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(mapPartitionsOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = output.<Integer>provideDataSet().collect();
        Assert.assertEquals(5, result.size());
        Assert.assertEquals(Arrays.asList(1, 2, 2, 3, 7), result);

    }

}
