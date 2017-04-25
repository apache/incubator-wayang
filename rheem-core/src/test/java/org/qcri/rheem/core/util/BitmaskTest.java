package org.qcri.rheem.core.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test suite for {@link Bitmask}s.
 */
public class BitmaskTest {

    private static Bitmask createBitmask(int capacity, int... bitIndices) {
        Bitmask bitmask = new Bitmask(capacity);
        for (int bitIndex : bitIndices) {
            bitmask.set(bitIndex);
        }
        return bitmask;
    }

    @Test
    public void testEquals() {
        Bitmask bitmask1 = createBitmask(64, 0, 3);
        Bitmask bitmask2 = createBitmask(256, 0, 3);
        Bitmask bitmask3 = createBitmask(256, 0, 3, 68);

        Assert.assertEquals(bitmask1, bitmask2);
        Assert.assertEquals(bitmask2, bitmask1);
        Assert.assertNotEquals(bitmask1, bitmask3);
        Assert.assertNotEquals(bitmask3, bitmask1);
        Assert.assertNotEquals(bitmask2, bitmask3);
        Assert.assertNotEquals(bitmask3, bitmask2);
    }

    @Test
    public void testFlip() {
        testFlip(0, 256);
        testFlip(32, 256);
        testFlip(32, 224);
        testFlip(65, 67);
    }

    private void testFlip(int from, int to) {
        Bitmask bitmask = createBitmask(0);
        for (int i = 0; i < 256; i += 2) {
            bitmask.set(i);
        }
        bitmask.flip(from, to);
        for (int i = 0; i < 256; i++) {
            boolean isEven = (i & 1) == 0;
            boolean isInFlipRanks = from <= i && i < to;
            boolean isExpectSetBit = isEven ^ isInFlipRanks;
            boolean isSetBit = bitmask.get(i);
            Assert.assertTrue(
                    String.format("Incorrect bit at %d in %s", i, bitmask),
                    isSetBit == isExpectSetBit
            );
        }
    }

    @Test
    public void testIsSubmaskOf() {
        Assert.assertTrue(createBitmask(0).isSubmaskOf(createBitmask(1, 0)));
        Assert.assertTrue(createBitmask(1).isSubmaskOf(createBitmask(1, 0)));
        Assert.assertFalse(createBitmask(1, 0).isSubmaskOf(createBitmask(0)));
        Assert.assertTrue(createBitmask(0, 1, 65).isSubmaskOf(createBitmask(0, 1, 65, 129)));
        Assert.assertTrue(createBitmask(0, 1, 129).isSubmaskOf(createBitmask(0, 1, 65, 129)));
        Assert.assertTrue(createBitmask(0, 1, 129).isSubmaskOf(createBitmask(0, 1, 65, 66, 129)));
        Assert.assertFalse(createBitmask(0, 1, 65, 66, 129).isSubmaskOf(createBitmask(0, 1, 65, 129)));
        Assert.assertTrue(createBitmask(0, 1, 129).isSubmaskOf(createBitmask(0, 1, 129)));
    }

    @Test
    public void testCardinality() {
        Assert.assertEquals(0, createBitmask(0).orInPlace(createBitmask(0)).cardinality());
        Assert.assertEquals(1, createBitmask(0, 65).orInPlace(createBitmask(0)).cardinality());
        Assert.assertEquals(2, createBitmask(0, 65, 66).orInPlace(createBitmask(0)).cardinality());
        Assert.assertEquals(3, createBitmask(0, 65, 66, 128).orInPlace(createBitmask(0)).cardinality());
    }

    @Test
    public void testOr() {
        Assert.assertEquals(createBitmask(0, 0), createBitmask(0).or(createBitmask(0, 0)));
        Assert.assertEquals(createBitmask(0, 0, 1), createBitmask(0, 1).or(createBitmask(0, 0)));
        Assert.assertEquals(createBitmask(0, 0, 1, 65, 128), createBitmask(0, 1, 128).or(createBitmask(0, 0, 65)));
        Assert.assertEquals(createBitmask(0, 0, 1, 65, 128), createBitmask(0, 0, 65).or(createBitmask(0, 1, 128)));
    }

    @Test
    public void testAndNot() {
        Assert.assertEquals(createBitmask(0), createBitmask(0).andNot(createBitmask(0, 0)));
        Assert.assertEquals(createBitmask(0, 1), createBitmask(0, 0, 1).andNot(createBitmask(0, 0)));
        Assert.assertEquals(createBitmask(0, 1, 128), createBitmask(0, 1, 128).andNot(createBitmask(0, 0, 65)));
        Assert.assertEquals(createBitmask(0, 65), createBitmask(0, 1, 65, 128).andNot(createBitmask(0, 1, 128)));
    }

    @Test
    public void testNextSetBit() {
        testSetBits();
        testSetBits(0);
        testSetBits(1);
        testSetBits(0, 1);
        testSetBits(420);
        testSetBits(1, 420);
        testSetBits(1, 420, 421, 500);
        testSetBits(1, 2, 3, 65);
    }

    private void testSetBits(int... setBits) {
        Bitmask bitmask = createBitmask(0, setBits);
        int i = 0;
        int nextBit = bitmask.nextSetBit(0);
        while (i < setBits.length) {
            Assert.assertEquals(setBits[i], nextBit);
            i++;
            nextBit = bitmask.nextSetBit(nextBit + 1);
        }
        Assert.assertEquals(-1, nextBit);
    }

}
