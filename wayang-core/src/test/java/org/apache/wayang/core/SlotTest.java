package org.apache.incubator.wayang.core;

import org.junit.Test;
import org.apache.incubator.wayang.core.plan.wayangplan.Slot;
import org.apache.incubator.wayang.core.plan.wayangplan.test.TestSink;
import org.apache.incubator.wayang.core.plan.wayangplan.test.TestSource;
import org.apache.incubator.wayang.core.test.TestDataUnit;
import org.apache.incubator.wayang.core.test.TestDataUnit2;
import org.apache.incubator.wayang.core.types.DataSetType;

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
