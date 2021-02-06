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

package org.apache.wayang.java.execution;

import org.junit.Assert;
import org.junit.Test;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.ExecutionContext;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.WayangArrays;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.operators.JavaCollectionSource;
import org.apache.wayang.java.operators.JavaDoWhileOperator;
import org.apache.wayang.java.operators.JavaLocalCallbackSink;
import org.apache.wayang.java.operators.JavaMapOperator;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

/**
 * Test suite for the {@link JavaExecutor}.
 */
public class JavaExecutorTest {

    @Test
    public void testLazyExecutionResourceHandling() {
        // The JavaExecutor should not dispose resources that are consumed by lazily executed ExecutionOperators until
        // execution.
        JavaCollectionSource<Integer> source1 = new JavaCollectionSource<>(
                Collections.singleton(1),
                DataSetType.createDefault(Integer.class)
        );
        source1.setName("source1");

        JavaCollectionSource<Integer> source2 = new JavaCollectionSource<>(
                WayangArrays.asList(2, 3, 4),
                DataSetType.createDefault(Integer.class)
        );
        source2.setName("source2");

        JavaDoWhileOperator<Integer, Integer> loop = new JavaDoWhileOperator<>(
                DataSetType.createDefault(Integer.class),
                DataSetType.createDefault(Integer.class),
                vals -> vals.stream().allMatch(v -> v > 5),
                5
        );
        loop.setName("loop");

        JavaMapOperator<Integer, Integer> increment = new JavaMapOperator<>(
                DataSetType.createDefault(Integer.class),
                DataSetType.createDefault(Integer.class),
                new TransformationDescriptor<>(
                        new FunctionDescriptor.ExtendedSerializableFunction<Integer, Integer>() {

                            private int increment;

                            @Override
                            public Integer apply(Integer integer) {
                                return integer + this.increment;
                            }

                            @Override
                            public void open(ExecutionContext ctx) {
                                this.increment = WayangCollections.getSingle(ctx.getBroadcast("inc"));
                            }
                        },
                        Integer.class, Integer.class
                )
        );
        increment.setName("increment");

        JavaMapOperator<Integer, Integer> id1 = new JavaMapOperator<>(
                DataSetType.createDefault(Integer.class),
                DataSetType.createDefault(Integer.class),
                new TransformationDescriptor<>(
                        v -> v,
                        Integer.class, Integer.class
                )
        );
        id1.setName("id1");

        JavaMapOperator<Integer, Integer> id2 = new JavaMapOperator<>(
                DataSetType.createDefault(Integer.class),
                DataSetType.createDefault(Integer.class),
                new TransformationDescriptor<>(
                        v -> v,
                        Integer.class, Integer.class
                )
        );
        id2.setName("id2");

        Collection<Integer> collector = new LinkedList<>();
        JavaLocalCallbackSink<Integer> sink = new JavaLocalCallbackSink<>(collector::add, DataSetType.createDefault(Integer.class));
        sink.setName("sink");

        loop.initialize(source2, 0);
        loop.beginIteration(increment, 0);
        source1.broadcastTo(0, increment, "inc");
        increment.connectTo(0, id1, 0);
        increment.connectTo(0, id2, 0);
        loop.endIteration(id1, 0, id2, 0);
        loop.outputConnectTo(sink, 0);

        final WayangContext wayangContext = new WayangContext().with(Java.basicPlugin());
        wayangContext.execute(new WayangPlan(sink));

        Assert.assertEquals(WayangArrays.asList(6, 7, 8), collector);
    }

}
