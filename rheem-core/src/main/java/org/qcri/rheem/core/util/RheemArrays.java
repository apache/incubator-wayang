package org.qcri.rheem.core.util;

/**
 * Utility for handling arrays.
 */
public class RheemArrays {

    private RheemArrays() { }

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
}
