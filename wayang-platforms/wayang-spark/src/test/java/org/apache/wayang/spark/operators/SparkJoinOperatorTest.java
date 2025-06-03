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

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;
import org.apache.wayang.spark.channels.RddChannel;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test suite for {@link SparkJoinOperator}.
 */
class SparkJoinOperatorTest extends SparkOperatorTestBase {

    @Test
    void testExecution() {
        // Prepare test data.
        RddChannel.Instance input0 = this.createRddChannelInstance(Arrays.asList(
                new Tuple2<>(1, "b"), new Tuple2<>(1, "c"), new Tuple2<>(2, "d"), new Tuple2<>(3, "e")));
        RddChannel.Instance input1 = this.createRddChannelInstance(Arrays.asList(
                new Tuple2<>("x", 1), new Tuple2<>("y", 1), new Tuple2<>("z", 2), new Tuple2<>("w", 4)));
        RddChannel.Instance output = this.createRddChannelInstance();

        // Build the Cartesian operator.
        SparkJoinOperator<Tuple2, Tuple2, Integer> join =
                new SparkJoinOperator<>(
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
                output.<Tuple2<Tuple2<Integer, String>, Tuple2<String, Integer>>>provideRdd().collect();
        assertEquals(5, result.size());
        assertEquals(new Tuple2<>(new Tuple2<>(1, "b"), new Tuple2<>("x", 1)), result.get(0));
        assertEquals(new Tuple2<>(new Tuple2<>(1, "b"), new Tuple2<>("y", 1)), result.get(1));
        assertEquals(new Tuple2<>(new Tuple2<>(1, "c"), new Tuple2<>("x", 1)), result.get(2));
        assertEquals(new Tuple2<>(new Tuple2<>(1, "c"), new Tuple2<>("y", 1)), result.get(3));
        assertEquals(new Tuple2<>(new Tuple2<>(2, "d"), new Tuple2<>("z", 2)), result.get(4));


    }

}
