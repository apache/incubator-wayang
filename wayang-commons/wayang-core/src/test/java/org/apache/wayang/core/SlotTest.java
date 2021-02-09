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

package org.apache.wayang.core;

import org.junit.Test;
import org.apache.wayang.core.plan.wayangplan.Slot;
import org.apache.wayang.core.plan.wayangplan.test.TestSink;
import org.apache.wayang.core.plan.wayangplan.test.TestSource;
import org.apache.wayang.core.test.TestDataUnit;
import org.apache.wayang.core.test.TestDataUnit2;
import org.apache.wayang.core.types.DataSetType;

/**
 * Test suite for {@link Slot}s.
 */
public class SlotTest {

    @Test(expected = IllegalArgumentException.class)
    public void testConnectMismatchingSlotFails() {
        TestSink<TestDataUnit> testSink = new TestSink<>(DataSetType.createDefault(TestDataUnit.class));
        TestSource<TestDataUnit2> testSource = new TestSource<>(DataSetType.createDefault(TestDataUnit2.class));
        testSource.connectTo(0, testSink, 0);
    }

    @Test
    public void testConnectMatchingSlots() {
        TestSink<TestDataUnit> testSink = new TestSink<>(DataSetType.createDefault(TestDataUnit.class));
        TestSource<TestDataUnit> testSource = new TestSource<>(DataSetType.createDefault(TestDataUnit.class));
        testSource.connectTo(0, testSink, 0);
    }
}
