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

import org.apache.wayang.core.function.FlatMapDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for {@link FlinkFlatMapOperator}.
 */
public class FlinkFlatMapOperatorTest extends FlinkOperatorTestBase{
    @Test
    public void testExecution() throws Exception {
        // Prepare test data.
        DataSetChannel.Instance input = this.createDataSetChannelInstance(Arrays.asList("one phrase", "two sentences", "three lines"));
        DataSetChannel.Instance output = this.createDataSetChannelInstance();

        FlinkFlatMapOperator<String, String> flatMapOperator = new FlinkFlatMapOperator<>(
                DataSetType.createDefaultUnchecked(String.class),
                DataSetType.createDefaultUnchecked(String.class),
                new FlatMapDescriptor<>(phrase -> Arrays.asList(phrase.split(" ")), String.class, String.class)
        );

        // Set up the ChannelInstances.
        ChannelInstance[] inputs = new ChannelInstance[]{input};
        ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(flatMapOperator, inputs, outputs);

        // Verify the outcome.
        final List<String> result = output.<String>provideDataSet().collect();
        Assert.assertEquals(6, result.size());
        Assert.assertEquals(Arrays.asList("one", "phrase", "two", "sentences", "three", "lines"), result);

    }
}
