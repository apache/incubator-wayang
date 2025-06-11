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

package org.apache.wayang.core.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test suite for {@link Bitmask}s.
 */
class BitmaskTest {

    private static Bitmask createBitmask(int capacity, int... bitIndices) {
        Bitmask bitmask = new Bitmask(capacity);
        for (int bitIndex : bitIndices) {
            bitmask.set(bitIndex);
        }
        return bitmask;
    }

    @Test
    void testEquals() {
        Bitmask bitmask1 = createBitmask(64, 0, 3);
        Bitmask bitmask2 = createBitmask(256, 0, 3);
        Bitmask bitmask3 = createBitmask(256, 0, 3, 68);

        assertEquals(bitmask1, bitmask2);
        assertEquals(bitmask2, bitmask1);
        assertNotEquals(bitmask1, bitmask3);
        assertNotEquals(bitmask3, bitmask1);
        assertNotEquals(bitmask2, bitmask3);
        assertNotEquals(bitmask3, bitmask2);
    }

    @Test
    void testFlip() {
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
            assertEquals(isSetBit, isExpectSetBit, String.format("Incorrect bit at %d in %s", i, bitmask));
        }
    }

    @Test
    void testIsSubmaskOf() {
        assertTrue(createBitmask(0).isSubmaskOf(createBitmask(1, 0)));
        assertTrue(createBitmask(1).isSubmaskOf(createBitmask(1, 0)));
        assertFalse(createBitmask(1, 0).isSubmaskOf(createBitmask(0)));
        assertTrue(createBitmask(0, 1, 65).isSubmaskOf(createBitmask(0, 1, 65, 129)));
        assertTrue(createBitmask(0, 1, 129).isSubmaskOf(createBitmask(0, 1, 65, 129)));
        assertTrue(createBitmask(0, 1, 129).isSubmaskOf(createBitmask(0, 1, 65, 66, 129)));
        assertFalse(createBitmask(0, 1, 65, 66, 129).isSubmaskOf(createBitmask(0, 1, 65, 129)));
        assertTrue(createBitmask(0, 1, 129).isSubmaskOf(createBitmask(0, 1, 129)));
    }

    @Test
    void testCardinality() {
        assertEquals(0, createBitmask(0).orInPlace(createBitmask(0)).cardinality());
        assertEquals(1, createBitmask(0, 65).orInPlace(createBitmask(0)).cardinality());
        assertEquals(2, createBitmask(0, 65, 66).orInPlace(createBitmask(0)).cardinality());
        assertEquals(3, createBitmask(0, 65, 66, 128).orInPlace(createBitmask(0)).cardinality());
    }

    @Test
    void testOr() {
        assertEquals(createBitmask(0, 0), createBitmask(0).or(createBitmask(0, 0)));
        assertEquals(createBitmask(0, 0, 1), createBitmask(0, 1).or(createBitmask(0, 0)));
        assertEquals(createBitmask(0, 0, 1, 65, 128), createBitmask(0, 1, 128).or(createBitmask(0, 0, 65)));
        assertEquals(createBitmask(0, 0, 1, 65, 128), createBitmask(0, 0, 65).or(createBitmask(0, 1, 128)));
    }

    @Test
    void testAndNot() {
        assertEquals(createBitmask(0), createBitmask(0).andNot(createBitmask(0, 0)));
        assertEquals(createBitmask(0, 1), createBitmask(0, 0, 1).andNot(createBitmask(0, 0)));
        assertEquals(createBitmask(0, 1, 128), createBitmask(0, 1, 128).andNot(createBitmask(0, 0, 65)));
        assertEquals(createBitmask(0, 65), createBitmask(0, 1, 65, 128).andNot(createBitmask(0, 1, 128)));
    }

    @Test
    void testNextSetBit() {
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
            assertEquals(setBits[i], nextBit);
            i++;
            nextBit = bitmask.nextSetBit(nextBit + 1);
        }
        assertEquals(-1, nextBit);
    }

}
