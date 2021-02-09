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

package org.apache.wayang.core.plan.wayangplan;

import org.junit.Assert;
import org.junit.Test;
import org.apache.wayang.core.plan.wayangplan.test.TestSink;
import org.apache.wayang.core.plan.wayangplan.test.TestSource;
import org.apache.wayang.core.types.DataSetType;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Test suite for {@link SlotMapping}.
 */
public class SlotMappingTest {

    final DataSetType<String> STRING_TYPE = DataSetType.createDefault(String.class);

    @Test
    public void testSimpleSlotMapping() {
        SlotMapping slotMapping = new SlotMapping();

        OutputSlot<String> innerOutputSlot1 = new OutputSlot<>("innerOutputSlot1", new TestSink<>(this.STRING_TYPE), this.STRING_TYPE);
        OutputSlot<String> outerOutputSlot1 = new OutputSlot<>("innerOutputSlot1", new TestSink<>(this.STRING_TYPE), this.STRING_TYPE);
        slotMapping.mapUpstream(outerOutputSlot1, innerOutputSlot1);

        InputSlot<String> outerInputSlot1 = new InputSlot<>("outerInputSlot1", new TestSource<>(this.STRING_TYPE), this.STRING_TYPE);
        InputSlot<String> innerInputSlot1 = new InputSlot<>("innerInputSlot1", new TestSource<>(this.STRING_TYPE), this.STRING_TYPE);
        slotMapping.mapUpstream(innerInputSlot1, outerInputSlot1);

        Assert.assertEquals(innerOutputSlot1, slotMapping.resolveUpstream(outerOutputSlot1));
        Assert.assertEquals(outerInputSlot1, slotMapping.resolveUpstream(innerInputSlot1));
    }

    @Test
    public void testOverridingSlotMapping() {
        SlotMapping slotMapping = new SlotMapping();

        OutputSlot<String> innerOutputSlot1 = new OutputSlot<>("innerOutputSlot1", new TestSink<>(this.STRING_TYPE), this.STRING_TYPE);
        OutputSlot<String> innerOutputSlot2 = new OutputSlot<>("innerOutputSlot2", new TestSink<>(this.STRING_TYPE), this.STRING_TYPE);
        OutputSlot<String> outerOutputSlot1 = new OutputSlot<>("innerOutputSlot1", new TestSink<>(this.STRING_TYPE), this.STRING_TYPE);
        slotMapping.mapUpstream(outerOutputSlot1, innerOutputSlot1);
        slotMapping.mapUpstream(outerOutputSlot1, innerOutputSlot2);

        InputSlot<String> outerInputSlot1 = new InputSlot<>("outerInputSlot1", new TestSource<>(this.STRING_TYPE), this.STRING_TYPE);
        InputSlot<String> outerInputSlot2 = new InputSlot<>("outerInputSlot2", new TestSource<>(this.STRING_TYPE), this.STRING_TYPE);
        InputSlot<String> innerInputSlot1 = new InputSlot<>("innerInputSlot1", new TestSource<>(this.STRING_TYPE), this.STRING_TYPE);
        slotMapping.mapUpstream(innerInputSlot1, outerInputSlot1);
        slotMapping.mapUpstream(innerInputSlot1, outerInputSlot2);

        Assert.assertEquals(innerOutputSlot2, slotMapping.resolveUpstream(outerOutputSlot1));
        Assert.assertEquals(outerInputSlot2, slotMapping.resolveUpstream(innerInputSlot1));
    }

    @Test
    public void testMultiMappings() {
        SlotMapping slotMapping = new SlotMapping();

        OutputSlot<String> innerOutputSlot1 = new OutputSlot<>("innerOutputSlot1", new TestSink<>(this.STRING_TYPE), this.STRING_TYPE);
        OutputSlot<String> outerOutputSlot1 = new OutputSlot<>("outerOutputSlot1", new TestSink<>(this.STRING_TYPE), this.STRING_TYPE);
        OutputSlot<String> outerOutputSlot2 = new OutputSlot<>("outerOutputSlot2", new TestSink<>(this.STRING_TYPE), this.STRING_TYPE);
        slotMapping.mapUpstream(outerOutputSlot1, innerOutputSlot1);
        slotMapping.mapUpstream(outerOutputSlot2, innerOutputSlot1);

        InputSlot<String> outerInputSlot1 = new InputSlot<>("outerInputSlot1", new TestSource<>(this.STRING_TYPE), this.STRING_TYPE);
        InputSlot<String> innerInputSlot1 = new InputSlot<>("innerInputSlot1", new TestSource<>(this.STRING_TYPE), this.STRING_TYPE);
        InputSlot<String> innerInputSlot2 = new InputSlot<>("innerInputSlot2", new TestSource<>(this.STRING_TYPE), this.STRING_TYPE);
        slotMapping.mapUpstream(innerInputSlot1, outerInputSlot1);
        slotMapping.mapUpstream(innerInputSlot2, outerInputSlot1);

        Assert.assertEquals(innerOutputSlot1, slotMapping.resolveUpstream(outerOutputSlot1));
        Assert.assertEquals(innerOutputSlot1, slotMapping.resolveUpstream(outerOutputSlot2));
        final Collection<OutputSlot<String>> outerOutputSlots = slotMapping.resolveDownstream(innerOutputSlot1);
        final List<OutputSlot<String>> expectedOuterOutputSlots = Arrays.asList(outerOutputSlot1, outerOutputSlot2);
        Assert.assertEquals(expectedOuterOutputSlots.size(), outerOutputSlots.size());
        Assert.assertTrue(expectedOuterOutputSlots.containsAll(outerOutputSlots));

        Assert.assertEquals(outerInputSlot1, slotMapping.resolveUpstream(innerInputSlot1));
        Assert.assertEquals(outerInputSlot1, slotMapping.resolveUpstream(innerInputSlot2));
        final Collection<InputSlot<String>> innerInputSlots = slotMapping.resolveDownstream(outerInputSlot1);
        final List<InputSlot<String>> expectedInnerInputSlots = Arrays.asList(innerInputSlot1, innerInputSlot2);
        Assert.assertEquals(expectedInnerInputSlots.size(), innerInputSlots.size());
        Assert.assertTrue(expectedInnerInputSlots.containsAll(innerInputSlots));
    }

}
