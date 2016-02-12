package org.qcri.rheem.core;

import org.junit.Test;
import org.qcri.rheem.core.plan.rheemplan.Slot;
import org.qcri.rheem.core.plan.rheemplan.test.TestSink;
import org.qcri.rheem.core.plan.rheemplan.test.TestSource;
import org.qcri.rheem.core.test.TestDataUnit;
import org.qcri.rheem.core.test.TestDataUnit2;
import org.qcri.rheem.core.types.DataSetType;

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
