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

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Utility for handling arrays.
 */
public class WayangArrays {

    private WayangArrays() { }

    /**
     * Enumerates in ascending order all integers {@code from <= i < to}.
     */
    public static int[] range(int from, int to) {
        assert from <= to;
        int[] result = new int[to - from];
        for (int i = from; i < to; i++) {
            result[i - from] = i;
        }
        return result;
    }

    /**
     * Enumerates in ascending order all integers {@code 0 <= i < to}.
     */
    public static int[] range(int to) {
        return range(0, to);
    }

    public static <T> boolean anyMatch(T[] array, Predicate<T> predicate) {
        for (T t : array) {
            if (predicate.test(t)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Converts {@code int} varargs into a {@link List}.
     */
    public static List<Integer> asList(int... values) {
        return Arrays.stream(values).mapToObj(Integer::valueOf).collect(Collectors.toList());
    }

    /**
     * Converts {@code long} varargs into a {@link List}.
     */
    public static List<Long> asList(long... values) {
        return Arrays.stream(values).mapToObj(Long::valueOf).collect(Collectors.toList());
    }

    /**
     * Convertes the {@code values} into a {@code long} array. This prohibits {@code null} values.
     */
    public static long[] toArray(Collection<Long> values) {
        final long[] array = new long[values.size()];
        int i = 0;
        for (Long value : values) {
            array[i++] = value;
        }
        return array;
    }

    /**
     * Converts a {@link BitSet} to an array that contains all indices set in this {@link BitSet}.
     */
    public static int[] toArray(BitSet bitSet) {
       int[] array = new int[bitSet.cardinality()];
       for (int i = 0, index = bitSet.nextSetBit(0);
               index != -1;
               i++, index = bitSet.nextSetBit(index + 1)) {
           array[i] = index;
       }
       return array;
    }
}
