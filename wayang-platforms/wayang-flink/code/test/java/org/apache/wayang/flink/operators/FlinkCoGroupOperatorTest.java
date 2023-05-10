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
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * Test suite for {@link FlinkCoGroupOperator}.
 */
public class FlinkCoGroupOperatorTest extends FlinkOperatorTestBase {

    @Test
    public void testExecution() throws Exception {
        // Prepare test data.
        DataSetChannel.Instance input0 = this.createDataSetChannelInstance(Arrays.asList(
                new Tuple2<>(1, "b"), new Tuple2<>(1, "c"), new Tuple2<>(2, "d"), new Tuple2<>(3, "e")));
        DataSetChannel.Instance input1 = this.createDataSetChannelInstance(Arrays.asList(
                new Tuple2<>("x", 1), new Tuple2<>("y", 1), new Tuple2<>("z", 2), new Tuple2<>("w", 4)));
        DataSetChannel.Instance output = this.createDataSetChannelInstance();

        // Build the operator.
        FlinkCoGroupOperator<Tuple2, Tuple2, Integer> coGroup =
                new FlinkCoGroupOperator<>(
                        new ProjectionDescriptor<>(
                                DataUnitType.createBasicUnchecked(Tuple2.class),
                                DataUnitType.createBasic(Integer.class),
                                "field0"),
                        new ProjectionDescriptor<>(
                                DataUnitType.createBasicUnchecked(Tuple2.class),
                                DataUnitType.createBasic(Integer.class),
                                "field1"),
                        DataSetType.createDefaultUnchecked(Tuple2.class),
                        DataSetType.createDefaultUnchecked(Tuple2.class)
                );

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{input0, input1};
        final ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(coGroup, inputs, outputs);

        // Verify the outcome.
        final List<Tuple2<Iterable<Tuple2<Integer, String>>, Iterable<Tuple2<String, Integer>>>> result =
                output.<Tuple2<Iterable<Tuple2<Integer, String>>, Iterable<Tuple2<String, Integer>>>>provideDataSet().collect();
        Collection<Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>>> expectedGroups =
                new ArrayList<>(Arrays.asList(
                        new Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>>(
                                Arrays.asList(new Tuple2<>(1, "b"), new Tuple2<>(1, "c")),
                                Arrays.asList(new Tuple2<>("x", 1), new Tuple2<>("y", 1))
                        ),
                        new Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>>(
                                Collections.singletonList(new Tuple2<>(2, "d")),
                                Collections.singletonList(new Tuple2<>("z", 2))
                        ), new Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>>(
                                Collections.singletonList(new Tuple2<>(3, "e")),
                                Collections.emptyList()
                        ),
                        new Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>>(
                                Collections.emptyList(),
                                Collections.singletonList(new Tuple2<>("w", 4))
                        )
                ));

        ResultLoop:
        for (Tuple2<Iterable<Tuple2<Integer, String>>, Iterable<Tuple2<String, Integer>>> resultCoGroup : result) {
            for (Iterator<Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>>> i = expectedGroups.iterator();
                 i.hasNext(); ) {
                Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>> expectedGroup = i.next();
                if (this.compare(expectedGroup, resultCoGroup)) {
                    i.remove();
                    continue ResultLoop;
                }
            }
            Assert.fail(String.format("Unexpected group: %s", resultCoGroup));
        }
        Assert.assertTrue(
                String.format("Missing groups: %s", expectedGroups),
                expectedGroups.isEmpty()
        );
    }

    private boolean compare(Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>> expected,
                            Tuple2<Iterable<Tuple2<Integer, String>>, Iterable<Tuple2<String, Integer>>> actual) {
        return this.compareGroup(expected.field0, actual.field0) && this.compareGroup(expected.field1, actual.field1);
    }

    private <T> boolean compareGroup(Collection<T> expected, Iterable<T> actual) {
        if (expected == null) return actual == null;
        if (actual == null) return false;

        return WayangCollections.asSet(expected).equals(WayangCollections.asSet(actual));
    }
}
