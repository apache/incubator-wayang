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

package org.apache.wayang.basic.mapping;

import org.junit.Assert;
import org.junit.Test;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.GroupByOperator;
import org.apache.wayang.basic.operators.ReduceByOperator;
import org.apache.wayang.basic.operators.ReduceOperator;
import org.apache.wayang.basic.operators.test.TestSink;
import org.apache.wayang.basic.operators.test.TestSource;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.plan.wayangplan.UnarySink;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;

/**
 * Test suite for the {@link ReduceByMapping}.
 */
public class ReduceByMappingTest {

    @Test
    public void testMapping() {
        // Construct a plan: source -> groupBy -> reduce -> sink.
        UnarySource<Tuple2<String, Integer>> source = new TestSource<>(DataSetType.createDefault(Tuple2.class));

        final ProjectionDescriptor<Tuple2<String, Integer>, String> keyDescriptor = new ProjectionDescriptor<>(
                DataUnitType.createBasicUnchecked(Tuple2.class),
                DataUnitType.createBasic(String.class),
                "field0");
        GroupByOperator<Tuple2<String, Integer>, String> groupBy = new GroupByOperator<>(
                keyDescriptor,
                DataSetType.createDefaultUnchecked(Tuple2.class),
                DataSetType.createGroupedUnchecked(Tuple2.class)
        );
        source.connectTo(0, groupBy, 0);

        final ReduceDescriptor<Tuple2<String, Integer>> reduceDescriptor = new ReduceDescriptor<>(
                (a, b) -> a, DataUnitType.createGroupedUnchecked(Tuple2.class),
                DataUnitType.createBasicUnchecked(Tuple2.class)
        );
        ReduceOperator<Tuple2<String, Integer>> reduce = ReduceOperator.createGroupedReduce(
                reduceDescriptor,
                DataSetType.createGroupedUnchecked(Tuple2.class),
                DataSetType.createDefaultUnchecked(Tuple2.class)
        );
        groupBy.connectTo(0, reduce, 0);

        UnarySink<Tuple2<String, Integer>> sink = new TestSink<>(DataSetType.createDefaultUnchecked(Tuple2.class));
        reduce.connectTo(0, sink, 0);
        WayangPlan plan = new WayangPlan();
        plan.addSink(sink);

        // Apply our mapping.
        Mapping mapping = new ReduceByMapping();
        for (PlanTransformation planTransformation : mapping.getTransformations()) {
            planTransformation.thatReplaces().transform(plan, Operator.FIRST_EPOCH + 1);
        }

        // Check that now we have this plan: source -> reduceBy -> sink.
        final Operator finalSink = plan.getSinks().iterator().next();
        final Operator inputOperator = finalSink.getEffectiveOccupant(0).getOwner();
        Assert.assertTrue(inputOperator instanceof ReduceByOperator);
        ReduceByOperator reduceBy = (ReduceByOperator) inputOperator;
        Assert.assertEquals(keyDescriptor, reduceBy.getKeyDescriptor());
        Assert.assertEquals(reduceDescriptor, reduceBy.getReduceDescriptor());
        Assert.assertEquals(source, reduceBy.getEffectiveOccupant(0).getOwner());
    }
}
