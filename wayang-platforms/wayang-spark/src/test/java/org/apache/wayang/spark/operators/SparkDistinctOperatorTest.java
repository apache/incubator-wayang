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

import org.junit.Assert;
import org.junit.Test;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.spark.channels.RddChannel;

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for {@link SparkDistinctOperator}.
 */
public class SparkDistinctOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        List<Integer> inputData = Arrays.asList(0, 1, 1, 6, 2, 2, 6, 6);

        // Build the distinct operator.
        SparkDistinctOperator<Integer> distinctOperator =
                new SparkDistinctOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class)
                );

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{this.createRddChannelInstance(inputData)};
        final ChannelInstance[] outputs = new ChannelInstance[]{this.createRddChannelInstance()};

        // Execute.
        this.evaluate(distinctOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = ((RddChannel.Instance) outputs[0]).<Integer>provideRdd().collect();
        Assert.assertEquals(4, result.size());
        Assert.assertEquals(Arrays.asList(0, 1, 6, 2), result);

    }

}
