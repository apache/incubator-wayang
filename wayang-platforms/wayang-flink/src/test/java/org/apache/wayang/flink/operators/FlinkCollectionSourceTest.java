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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


/**
 * Test suite for the {@link FlinkCollectionSource}.
 */
public class FlinkCollectionSourceTest extends FlinkOperatorTestBase{
    @Test
    public void testExecution() throws Exception {
        Set<Integer> inputValues = new HashSet<>(Arrays.asList(1, 2, 3));
        FlinkCollectionSource<Integer> collectionSource = new FlinkCollectionSource<Integer>(
                inputValues,
                DataSetType.createDefault(Integer.class));
        DataSetChannel.Instance output = this.createDataSetChannelInstance();

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{};
        final ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(collectionSource, inputs, outputs);

        final Set<Integer> outputValues = new HashSet<>(output.<Integer>provideDataSet().collect());
        Assert.assertEquals(outputValues, inputValues);
    }
}
