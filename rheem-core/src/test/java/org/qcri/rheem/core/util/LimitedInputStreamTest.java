package org.qcri.rheem.core.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Test suite for the {@link LimitedInputStream}.
 */
public class LimitedInputStreamTest {

    @Test
    public void testLimitation() throws IOException {
        // Generate test data.
        byte[] testData = new byte[(int) Byte.MAX_VALUE + 1];
        for (int b = 0; b <= Byte.MAX_VALUE; b++) {
            testData[b] = (byte) b;
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(testData);

        final int limit = 42;
        LimitedInputStream lis = new LimitedInputStream(bais, limit);

        for (int i = 0; i < limit; i++) {
            Assert.assertEquals(i, lis.getNumReadBytes());
            Assert.assertEquals(i, lis.read());
        }
        Assert.assertEquals(42, lis.getNumReadBytes());
        Assert.assertEquals(-1, lis.read());
        Assert.assertEquals(42, lis.getNumReadBytes());
    }

}
