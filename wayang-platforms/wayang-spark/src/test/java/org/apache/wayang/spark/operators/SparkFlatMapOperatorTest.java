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
import org.apache.wayang.core.function.FlatMapDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.spark.channels.RddChannel;

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for {@link SparkFilterOperator}.
 */
public class SparkFlatMapOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        RddChannel.Instance input = this.createRddChannelInstance(Arrays.asList("one phrase", "two sentences", "three lines"));
        RddChannel.Instance output = this.createRddChannelInstance();

        SparkFlatMapOperator<String, String> flatMapOperator = new SparkFlatMapOperator<>(
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
        final List<String> result = output.<String>provideRdd().collect();
        Assert.assertEquals(6, result.size());
        Assert.assertEquals(Arrays.asList("one", "phrase", "two", "sentences", "three", "lines"), result);

    }

}
