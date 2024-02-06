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
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Test suite for {@link FlinkGlobalMaterializedGroupOperator}.
 */
public class FlinkGlobalMaterializedGroupOperatorTest extends FlinkOperatorTestBase{

    @Test
    public void testExecution() throws Exception {
        // Prepare test data.
        Collection<Integer> inputCollection = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // Build the reduce operator.
        FlinkGlobalMaterializedGroupOperator<Integer> globalGroup =
                new FlinkGlobalMaterializedGroupOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class),
                        DataSetType.createGroupedUnchecked(Iterable.class)
                );

        // Execute.
        ChannelInstance[] inputs = new DataSetChannel.Instance[]{this.createDataSetChannelInstance(inputCollection)};
        ChannelInstance[] outputs = new DataSetChannel.Instance[]{this.createDataSetChannelInstance()};
        this.evaluate(globalGroup, inputs, outputs);

        // Verify the outcome.
        final Collection<Iterable<Integer>> result = ((DataSetChannel.Instance) outputs[0]).<Iterable<Integer>>provideDataSet().collect();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(inputCollection, result.iterator().next());

    }

}
