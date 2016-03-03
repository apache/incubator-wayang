package org.qcri.rheem.core.util;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Utility to measure time spans.
 */
public class StopWatch {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final Map<String, Round> rounds = new LinkedHashMap<>();

    /**
     * Starts a new round. Any required parent rounds will be started if necessary.
     *
     * @param fullyQualifiedName the fully qualified name of the round
     * @return the new {@link Round}
     */
    public Round start(String... fullyQualifiedName) {
        Validate.isTrue(fullyQualifiedName.length > 0);

        if (fullyQualifiedName.length == 1) {
            return this.startSubround(this.rounds, fullyQualifiedName[0], null);
        }
        Round round = this.getOrStartSubround(this.rounds, fullyQualifiedName[0], null);
        for (int i = 1; i < fullyQualifiedName.length - 1; i++) {
            round = round.getOrStartSubround(fullyQualifiedName[i]);
        }

        return round.startSubround(fullyQualifiedName[fullyQualifiedName.length - 1]);
    }

    /**
     * Retrieves a round.
     *
     * @param fullyQualifiedName the fully qualified name of the round
     * @return the new {@link Round} or {@code null} if no such round
     */
    public Round get(String... fullyQualifiedName) {
        Validate.isTrue(fullyQualifiedName.length > 0);

        Round round = this.getSubround(this.rounds, fullyQualifiedName[0]);
        for (int i = 1; i < fullyQualifiedName.length; i++) {
            if (round == null) return null;
            round = round.getSubround(fullyQualifiedName[i]);
        }

        return round;
    }


    /**
     * Stops a round (but not any nested subrounds).
     *
     * @param fullyQualifiedName the fully qualified name of the round
     * @return the stopped {@link Round} or {@code null} if no such round
     */
    public Round stop(String... fullyQualifiedName) {
        final Round round = this.get(fullyQualifiedName);
        if (round != null) {
            round.stop(false);
        }

        return round;
    }

    /**
     * Stops a round and any nested subrounds.
     *
     * @param fullyQualifiedName the fully qualified name of the round; leave blank to stop the complete instance
     * @return the stopped {@link Round} or {@code null} if no such round
     */
    public Round stopAll(String... fullyQualifiedName) {
        if (fullyQualifiedName.length == 0) {
            this.rounds.values().forEach(round -> round.stop(true, true));
            return null;
        }

        final Round round = this.get(fullyQualifiedName);
        if (round != null) {
            round.stop(true, true);
        }

        return round;
    }

    private Round startSubround(Map<String, Round> rounds, String name, Round parentRound) {
        final Round round = new Round(name, parentRound);
        final Round formerRound = rounds.put(name, round);
        if (formerRound != null) {
            this.logger.warn("Restarted {}.", formerRound.getFullName());
        }
        return round;
    }

    private Round getSubround(Map<String, Round> rounds, String name) {
        return rounds.get(name);
    }

    private Round getOrStartSubround(Map<String, Round> rounds, String name, Round parentRound) {
        return rounds.computeIfAbsent(name, (nameKey) -> new Round(nameKey, parentRound));
    }

    public String toPrettyString() {
        return this.toPrettyString("  ", "* ");
    }

    public String toPrettyString(String indent, String bullet) {
        StringBuilder sb = new StringBuilder(1000);
        int firstColumnWidth = this.determineFirstColumnWidth(this.rounds, indent.length(), 0, bullet.length());
        this.rounds.values().forEach(round -> round.append(indent, 0, bullet, firstColumnWidth, sb));
        // Trim trailing newline.
        if (sb.length() > 0 && sb.charAt(sb.length() - 1) == '\n') {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    private int determineFirstColumnWidth(Map<String, Round> rounds, int indentWidth, int numIndents, int bulletWidth) {
        return rounds.values().stream()
                .map(round -> this.determineFirstColumnWidth(round, indentWidth, numIndents, bulletWidth))
                .reduce(0, Math::max);
    }

    private int determineFirstColumnWidth(Round round, int indentWidth, int numIndents, int bulletWidth) {
        return Math.max(
                round.getName().length() + (numIndents * indentWidth) + bulletWidth,
                this.determineFirstColumnWidth(round.subrounds, indentWidth, numIndents + 1, bulletWidth)
        );
    }

    /**
     * Represents a single measurement of a time span. Can have nested instances.
     */
    public class Round {

        private final Map<String, Round> subrounds = new LinkedHashMap<>(4);

        private final Round parent;

        private final long startTime = System.currentTimeMillis();

        private long stopTime = -1;

        private final String name;

        public Round(String name, Round parent) {
            this.parent = parent;
            this.name = name;
        }

        public Round startSubround(String name) {
            return StopWatch.this.startSubround(this.subrounds, name, this);
        }


        public Round getSubround(String name) {
            return StopWatch.this.getSubround(this.subrounds, name);
        }

        public Round getOrStartSubround(String name) {
            return StopWatch.this.getOrStartSubround(this.subrounds, name, this);
        }

        public long stopSubround(String name) {
            final Round subround = this.getSubround(name);
            if (subround != null) {
                return subround.stop();
            }
            return -1;
        }


        /**
         * @return the fully qualified name of this round
         */
        public String getFullName() {
            return this.getFullName("->");
        }

        /**
         * @param separator separates round names in the fully qualified name
         * @return the fully qualified name of this round
         */
        public String getFullName(String separator) {
            final StringBuilder sb = new StringBuilder(50);
            this.putFullName(sb, separator);
            return sb.toString();
        }

        private void putFullName(StringBuilder sb, String separator) {
            if (this.parent != null) {
                this.parent.putFullName(sb, separator);
                sb.append(separator);
            }
            sb.append(this.getName());
        }

        /**
         * @return the name of this very round
         */
        public String getName() {
            return this.name;
        }

        /**
         * If not yet stopped, stop this round.
         *
         * @return the measured millis
         */
        public long stop() {
            return this.stop(false, false);
        }
        /**
         * If not yet stopped, stop this round.
         *
         * @return the measured millis
         */
        public long stop(boolean stopSubrounds) {
            return this.stop(stopSubrounds, false);
        }


        /**
         * If not yet stopped, stop this round.
         *
         * @return the measured millis
         */
        public long stop(boolean stopSubrounds, boolean suppressWarnings) {
            if (this.stopTime != -1) {
                if (!suppressWarnings) StopWatch.this.logger.warn("{} has already been stopped.", this.getFullName());
            } else {
                this.stopTime = System.currentTimeMillis();
            }

            if (stopSubrounds) {
                this.subrounds.values().forEach(subround -> subround.stop(true));
            }

            return this.stopTime - this.startTime;
        }


        private void append(String indent, int numIndents, String bullet, int firstColumnWidth, StringBuilder sb) {
            int originalLength = sb.length();
            for (int i = 0; i < numIndents; i++) {
                sb.append(indent);
            }
            sb.append(bullet).append(this.getName());
            while (sb.length() < originalLength + firstColumnWidth) sb.append(" ");
            sb.append(" - ").append(Formats.formatDuration(this.getDuration())).append('\n');
            this.subrounds.values().forEach(round -> round.append(indent, numIndents + 1, bullet, firstColumnWidth, sb));
        }

        public long getDuration() {
            return this.stopTime == -1 ? -1 : this.stopTime - this.startTime;
        }
    }

}
