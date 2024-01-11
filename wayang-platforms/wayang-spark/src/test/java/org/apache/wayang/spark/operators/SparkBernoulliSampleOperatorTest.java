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
import org.junit.Ignore;
import org.junit.Test;
import org.apache.wayang.basic.operators.SampleOperator;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.spark.channels.RddChannel;

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for {@link SparkBernoulliSampleOperator}.
 */
public class SparkBernoulliSampleOperatorTest extends SparkOperatorTestBase {

    @Ignore("Cannot check this, because the sample size returned is not always exact.")
    @Test
    public void testExecution() {
        // Prepare test data.
        List<Integer> inputData = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // Build the distinct operator.
        SparkBernoulliSampleOperator<Integer> sampleOperator =
                new SparkBernoulliSampleOperator<>(
                        iterationNumber -> 5,
                        DataSetType.createDefaultUnchecked(Integer.class),
                        iterationNumber -> SampleOperator.randomSeed()
                );

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{this.createRddChannelInstance(inputData)};
        final ChannelInstance[] outputs = new ChannelInstance[]{this.createRddChannelInstance()};

        // Execute.
        this.evaluate(sampleOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = ((RddChannel.Instance) outputs[0]).<Integer>provideRdd().collect();
        Assert.assertEquals(5, result.size());
    }

    @Test
    public void testDoesNotFail() {
        // Prepare test data.
        List<Integer> inputData = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // Build the distinct operator.
        SparkBernoulliSampleOperator<Integer> sampleOperator =
                new SparkBernoulliSampleOperator<>(
                        iterationNumber -> 5,
                        DataSetType.createDefaultUnchecked(Integer.class),
                        iterationNumber -> SampleOperator.randomSeed()
                );

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{this.createRddChannelInstance(inputData)};
        final ChannelInstance[] outputs = new ChannelInstance[]{this.createRddChannelInstance()};

        // Execute.
        this.evaluate(sampleOperator, inputs, outputs);

        // Verify the outcome.
        ((RddChannel.Instance) outputs[0]).<Integer>provideRdd().collect();

    }

}
