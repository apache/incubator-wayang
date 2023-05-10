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
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;


/**
 * Test suite for {@link FlinkJoinOperator}.
 */
public class FlinkJoinOperatorTest extends FlinkOperatorTestBase{

    //TODO: Validate FlinkJoinOperator implementation
    // it is required to validate the implementation of FlinkJoinOperator
    // because trigger an exception in the test and looks like is a problem in the
    // implementation of the implementation in the operator
    // labels:flink,bug
    @Test
    public void testExecution() throws Exception {
        // Prepare test data.
        DataSetChannel.Instance input0 = this.createDataSetChannelInstance(Arrays.asList(
                new Tuple2<>(1, "b"), new Tuple2<>(1, "c"), new Tuple2<>(2, "d"), new Tuple2<>(3, "e")));
        DataSetChannel.Instance input1 = this.createDataSetChannelInstance(Arrays.asList(
                new Tuple2<>("x", 1), new Tuple2<>("y", 1), new Tuple2<>("z", 2), new Tuple2<>("w", 4)));
        DataSetChannel.Instance output = this.createDataSetChannelInstance();

        // Build the Cartesian operator.
        FlinkJoinOperator<Tuple2, Tuple2, Integer> join =
                new FlinkJoinOperator<>(
                        DataSetType.createDefaultUnchecked(Tuple2.class),
                        DataSetType.createDefaultUnchecked(Tuple2.class),
                        new ProjectionDescriptor<>(
                                DataUnitType.createBasicUnchecked(Tuple2.class),
                                DataUnitType.createBasic(Integer.class),
                                "field0"),
                        new ProjectionDescriptor<>(
                                DataUnitType.createBasicUnchecked(Tuple2.class),
                                DataUnitType.createBasic(Integer.class),
                                "field1"));

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{input0, input1};
        final ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(join, inputs, outputs);

        // Verify the outcome.
        final List<Tuple2<Tuple2<Integer, String>, Tuple2<String, Integer>>> result =
                output.<Tuple2<Tuple2<Integer, String>, Tuple2<String, Integer>>>provideDataSet().collect();
        Assert.assertEquals(5, result.size());
        Assert.assertEquals(result.get(0), new Tuple2<>(new Tuple2<>(1, "b"), new Tuple2<>("x", 1)));
        Assert.assertEquals(result.get(1), new Tuple2<>(new Tuple2<>(1, "b"), new Tuple2<>("y", 1)));
        Assert.assertEquals(result.get(2), new Tuple2<>(new Tuple2<>(1, "c"), new Tuple2<>("x", 1)));
        Assert.assertEquals(result.get(3), new Tuple2<>(new Tuple2<>(1, "c"), new Tuple2<>("y", 1)));
        Assert.assertEquals(result.get(4), new Tuple2<>(new Tuple2<>(2, "d"), new Tuple2<>("z", 2)));


    }
}
