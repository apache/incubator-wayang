package org.qcri.rheem.core.util;

/**
 * Formats different general purpose objects.
 */
public class Formats {

    /**
     * Formats the given milliseconds as {@code h:MM:ss.mmm}.
     *
     * @param millis the milliseconds to format
     * @return the formatted milliseconds
     */
    public static String formatDuration(long millis) {
        if (millis < 0) return "-" + formatDuration(-millis);
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
     * Formats the given milliseconds as {@code h:MM:ss.mmm (millis ms)}.
     *
     * @param millis       the milliseconds to format
     * @param isProvideRaw whether to include the {@code (millis ms)}
     * @return the formatted milliseconds
     */
    public static String formatDuration(long millis, boolean isProvideRaw) {
        return isProvideRaw ?
                String.format("%s (%d ms)", formatDuration(millis), millis) :
                formatDuration(millis);
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

    /**
     * Formats a large number by replacing less significant digits with G, M, or K.
     *
     * @param val the value to format
     * @return the simplified value
     */
    public static String formatLarge(long val) {
        StringBuilder sb = new StringBuilder(10);
        if (val < 0) sb.append("-");
        val = Math.abs(val);
        if (val >= 1000000000L) {
            sb.append(val / 1000000000L).append("G");
        } else if (val >= 1000000L) {
            sb.append(val / 1000000L).append("M");
        } else if (val >= 1000L) {
            sb.append(val / 1000L).append("K");
        } else {
            sb.append(val);
        }
        return sb.toString();
    }
}
