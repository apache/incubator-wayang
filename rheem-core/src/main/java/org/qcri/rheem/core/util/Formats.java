package org.qcri.rheem.core.util;

/**
 * Formats different general purpose objects.
 */
public class Formats {

    public static String formatDuration(long millis) {
        if (millis < 0) return "-:--:--.---";
        long ms = millis % 1000;
        millis /= 1000;
        long s = millis % 60;
        millis /= 60;
        long m = millis % 60;
        millis /= 60;
        long h = millis % 60;
        return String.format("%d:%02d:%02d.%03d", h, m, s, ms);
    }


    /**
     * Formats a value in {@code [0, 1]} as a percentage with 2 decimal places.
     *
     * @param val value to be formated
     * @return formatted {@link String}
     */
    public static String formatPercentage(double val) {
        return String.format("%.2f%%", val * 100d);
    }
}
