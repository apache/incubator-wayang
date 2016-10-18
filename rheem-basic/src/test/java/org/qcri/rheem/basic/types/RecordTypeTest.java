package org.qcri.rheem.basic.types;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.core.types.DataSetType;

/**
 * Tests for the {@link RecordType}.
 */
public class RecordTypeTest {

    @Test
    public void testSupertype() {
        DataSetType<Record> t1 = DataSetType.createDefault(Record.class);
        DataSetType<Record> t2 = DataSetType.createDefault(new RecordType("a", "b"));
        DataSetType<Record> t3 = DataSetType.createDefault(new RecordType("a", "b", "c"));

        Assert.assertTrue(t1.isSupertypeOf(t2));
        Assert.assertFalse(t2.isSupertypeOf(t1));
        Assert.assertTrue(t1.isSupertypeOf(t3));
        Assert.assertFalse(t3.isSupertypeOf(t1));
        Assert.assertTrue(t2.isSupertypeOf(t2));
        Assert.assertFalse(t2.isSupertypeOf(t3));
        Assert.assertTrue(t3.isSupertypeOf(t3));
        Assert.assertFalse(t3.isSupertypeOf(t2));
    }
}
