/*
 * Copyright 2016 Sebastian Kruse,
 * code from: https://github.com/sekruse/profiledb-java.git
 * Original license:
 * Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.wayang.commons.util.profiledb.model.measurement;

import org.apache.wayang.commons.util.profiledb.model.Measurement;
import org.apache.wayang.commons.util.profiledb.model.Type;

import java.util.Collection;
import java.util.LinkedList;

/**
 * A {@link Measurement} that captures a certain amount of time in milliseconds. Instances can be nested within
 * each other.
 * <p>Besides storing those data, it also provides utility functionality to obtain measurements.</p>
 */
@Type("time")
public class TimeMeasurement extends Measurement {

    /**
     * The measured time in milliseconds.
     */
    private long millis = 0L;

    /**
     * Keeps track on measurement starts.
     */
    private transient long startTime = -1L;

    /**
     * Sub-{@link TimeMeasurement}s of this instance.
     */
    private Collection<TimeMeasurement> rounds = new LinkedList<>();

    /**
     * Serialization constructor.
     */
    @SuppressWarnings("unused")
    private TimeMeasurement() {
        super();
    }

    /**
     * Creates a new instance.
     *
     * @param id the ID of the new instance
     */
    public TimeMeasurement(String id) {
        super(id);
    }

    /**
     * Start measuring time for this instance.
     */
    public void start() {
        this.startTime = System.currentTimeMillis();
    }

    /**
     * Ensure that this instance has its timer started.
     */
    private void ensureStarted() {
        if (this.startTime == -1L) {
            this.startTime = System.currentTimeMillis();
        }
    }

    /**
     * Start a (potentially new) sub-{@link TimeMeasurement}.
     *
     * @param identifiers identifies the target {@link TimeMeasurement} as a path of IDs
     * @return the started instance
     */
    public TimeMeasurement start(String... identifiers) {
        return this.start(identifiers, 0);
    }

    /**
     * Start a (potentially new) sub-{@link TimeMeasurement}.
     *
     * @param identifiers identifies the target {@link TimeMeasurement} as a path of IDs
     * @param index       the index of this instance within {@code identifiers}
     * @return the started instance
     */
    private TimeMeasurement start(String[] identifiers, int index) {
        if (index >= identifiers.length) {
            this.start();
            return this;
        } else {
            this.ensureStarted();
            TimeMeasurement round = this.getOrCreateRound(identifiers[index]);
            return round.start(identifiers, index + 1);
        }
    }

    /**
     * Retrieves an existing {@link TimeMeasurement} from {@link #rounds} with the given {@code id} or creates and stores a new one.
     *
     * @param id the ID of the {@link TimeMeasurement}
     * @return the {@link TimeMeasurement}
     */
    public TimeMeasurement getOrCreateRound(String id) {
        TimeMeasurement round = this.getRound(id);
        if (round != null) return round;

        round = new TimeMeasurement(id);
        this.rounds.add(round);
        return round;
    }

    /**
     * Retrieves an existing {@link TimeMeasurement} from {@link #rounds} with the given {@code id}.
     *
     * @param id the ID of the {@link TimeMeasurement}
     * @return the {@link TimeMeasurement} or {@code null} if it does not exist
     */
    private TimeMeasurement getRound(String id) {
        for (TimeMeasurement round : this.rounds) {
            if (id.equals(round.getId())) {
                return round;
            }
        }
        return null;
    }

    /**
     * Stop a measurement that has been started via {@link #start()} or derivatives.
     */
    public void stop() {
        this.stop(System.currentTimeMillis());
    }


    /**
     * Stop a measurement that has been started via {@link #start()} or derivatives.
     *
     * @param stopTime at which the measurement has been stopped
     */
    private void stop(long stopTime) {
        if (this.startTime != -1L) {
            this.millis += (stopTime - this.startTime);
            this.startTime = -1L;
        }
        for (TimeMeasurement round : this.rounds) {
            round.stop(stopTime);
        }
    }

    /**
     * Stop a measurement that has been started via {@link #start(String...)} or related.
     *
     * @param identfiers identify the target {@link TimeMeasurement} as a path of IDs
     */
    public void stop(String... identfiers) {
        long stopTime = System.currentTimeMillis();
        TimeMeasurement round = this;
        for (String identfier : identfiers) {
            round = round.getRound(identfier);
            if (round == null) return;
        }
        round.stop(stopTime);
    }

    public long getMillis() {
        return millis;
    }

    public void setMillis(long millis) {
        this.millis = millis;
    }

    public Collection<TimeMeasurement> getRounds() {
        return rounds;
    }

    public void addRounds(TimeMeasurement round) {
        this.rounds.add(round);
    }

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

    @Override
    public String toString() {
        return String.format("%s[%s, %s, %d subs]", this.getClass().getSimpleName(), this.getId(), formatDuration(this.millis), this.rounds.size());
    }
}
