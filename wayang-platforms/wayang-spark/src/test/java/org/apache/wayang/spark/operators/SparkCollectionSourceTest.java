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
import java.util.HashSet;
import java.util.Set;

/**
 * Test suite for the {@link SparkCollectionSource}.
 */
public class SparkCollectionSourceTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        Set<Integer> inputValues = new HashSet<>(Arrays.asList(1, 2, 3));
        SparkCollectionSource<Integer> collectionSource = new SparkCollectionSource<>(
                inputValues,
                DataSetType.createDefault(Integer.class));
        RddChannel.Instance output = this.createRddChannelInstance();

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{};
        final ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(collectionSource, inputs, outputs);

        final Set<Integer> outputValues = new HashSet<>(output.<Integer>provideRdd().collect());
        Assert.assertEquals(outputValues, inputValues);
    }
}
