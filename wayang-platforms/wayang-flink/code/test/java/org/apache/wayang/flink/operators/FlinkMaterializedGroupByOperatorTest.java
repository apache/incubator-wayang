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
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Test suite for {@link FlinkMaterializedGroupByOperator}.
 */
public class FlinkMaterializedGroupByOperatorTest extends FlinkOperatorTestBase {

    @Test
    @SuppressWarnings("unchecked")
    public void testExecution() throws Exception {
        // Prepare test data.
        AtomicInteger counter = new AtomicInteger(0);
        DataSetChannel.Instance input = this.createDataSetChannelInstance(Arrays.stream("abcaba".split(""))
                .map(string -> new Tuple2<>(string, counter.getAndIncrement()))
                .collect(Collectors.toList()));
        DataSetChannel.Instance output = this.createDataSetChannelInstance();

        // Build the reduce operator.
        FlinkMaterializedGroupByOperator<Tuple2<String, Integer>, String> collocateByOperator =
                new FlinkMaterializedGroupByOperator<>(
                        new ProjectionDescriptor<>(
                                DataUnitType.createBasicUnchecked(Tuple2.class),
                                DataUnitType.createBasicUnchecked(String.class),
                                "field0"
                        ),
                        DataSetType.createDefaultUnchecked(Tuple2.class),
                        DataSetType.createGroupedUnchecked(String.class)
                );

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{input};
        final ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        try {
            this.evaluate(collocateByOperator, inputs, outputs);
        }
        catch (Exception e){
            e.printStackTrace();
            Assert.fail();
        }

        // Verify the outcome.
        final List<Iterable<Tuple2<String, Integer>>> originalResult =
                output.<Iterable<Tuple2<String, Integer>>>provideDataSet().collect();
        Set<List<Tuple2<String, Integer>>> result = originalResult.stream()
                .map(this::toList)
                .collect(Collectors.toSet());

        final List[] expectedResults = {
                Arrays.asList(new Tuple2<>("a", 0), new Tuple2<>("a", 3), new Tuple2<>("a", 5)),
                Arrays.asList(new Tuple2<>("b", 1), new Tuple2<>("b", 4)),
                Arrays.asList(new Tuple2<>("c", 2))
        };
        Arrays.stream(expectedResults)
                .forEach(expected -> Assert.assertTrue("Not contained: " + expected, result.contains(expected)));
        Assert.assertEquals(expectedResults.length, result.size());

    }

    private <T> List<T> toList(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList());
    }

}
