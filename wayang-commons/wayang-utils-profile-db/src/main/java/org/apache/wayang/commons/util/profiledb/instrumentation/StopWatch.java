/*
 * Copyright 2016 Sebastian Kruse,
 * code from: https://github.com/sekruse/profiledb-java.git
 * Original license:
 * Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.wayang.commons.util.profiledb.instrumentation;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.wayang.commons.util.profiledb.model.Experiment;
import org.apache.wayang.commons.util.profiledb.model.Measurement;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;

/**
 * Utility to create {@link TimeMeasurement}s for an {@link Experiment}.
 */
public class StopWatch {

  private final Map<String, TimeMeasurement> rounds = new LinkedHashMap<>();

  private final Experiment experiment;

  /**
   * Creates a new instance for the given {@link Experiment}.
   *
   * @param experiment that should be instrumented
   */
  public StopWatch(Experiment experiment) {
    this.experiment = experiment;
    for (Measurement measurement : this.experiment.getMeasurements()) {
      if (measurement instanceof TimeMeasurement) {
        this.rounds.put(measurement.getId(), (TimeMeasurement) measurement);
      }
    }
  }

  /**
   * Retrieves a {@link TimeMeasurement} by its ID from {@link #rounds}. If it does not exist, it will be
   * created and also registered with the {@link #experiment}.
   *
   * @param id         the ID of the {@link TimeMeasurement}
   * @param furtherIds IDs of further nested {@link TimeMeasurement}s
   * @return the {@link TimeMeasurement}
   */
  public TimeMeasurement getOrCreateRound(String id, String... furtherIds) {
    TimeMeasurement round = this.rounds.get(id);
    if (round == null) {
      round = new TimeMeasurement(id);
      this.rounds.put(round.getId(), round);
      this.experiment.addMeasurement(round);
    }
    for (String furtherId : furtherIds) {
      round = round.getOrCreateRound(furtherId);
    }
    return round;
  }

  /**
   * Starts a {@link TimeMeasurement}. Any required parent {@link TimeMeasurement} will be started if necessary.
   *
   * @param id         ID of the root {@link TimeMeasurement}
   * @param furtherIds IDs of further nested {@link TimeMeasurement}s
   * @return the (potentially new) {@link TimeMeasurement}
   */
  public TimeMeasurement start(String id, String... furtherIds) {
    return this.getOrCreateRound(id).start(furtherIds);
  }

  /**
   * Stops a {@link TimeMeasurement} (if it exists).
   *
   * @param id         ID of the root {@link TimeMeasurement}
   * @param furtherIds IDs of further nested {@link TimeMeasurement}s
   */
  public void stop(String id, String... furtherIds) {
    final TimeMeasurement rootRound = this.rounds.get(id);
    if (rootRound != null) {
      rootRound.stop(furtherIds);
    }
  }

  /**
   * Formats the {@link TimeMeasurement}s to something easily readable.
   *
   * @return the "pretty" {@link String}
   */
  public String toPrettyString() {
    return this.toPrettyString("  ", "* ");
  }

  /**
   * Formats the {@link TimeMeasurement}s to something easily readable.
   *
   * @param indent indent to be used when formatting sub-{@link TimeMeasurement}s
   * @param bullet bullet point character
   * @return the "pretty" {@link String}
   */
  public String toPrettyString(String indent, String bullet) {
    StringBuilder sb = new StringBuilder(1000);
    int firstColumnWidth = this.determineFirstColumnWidth(this.rounds.values(), indent.length(), 0, bullet.length());
    this.rounds.values().forEach(round -> this.append(round, indent, 0, bullet, firstColumnWidth, sb));
    // Trim trailing newline.
    if (sb.length() > 0 && sb.charAt(sb.length() - 1) == '\n') {
      sb.setLength(sb.length() - 1);
    }
    return sb.toString();
  }

  private int determineFirstColumnWidth(Collection<TimeMeasurement> rounds, int indentWidth, int numIndents, int bulletWidth) {
    return rounds.stream()
        .map(round -> this.determineFirstColumnWidth(round, indentWidth, numIndents, bulletWidth))
        .reduce(0, Math::max);
  }

  private int determineFirstColumnWidth(TimeMeasurement round, int indentWidth, int numIndents, int bulletWidth) {
    return Math.max(
        round.getId().length() + (numIndents * indentWidth) + bulletWidth,
        this.determineFirstColumnWidth(round.getRounds(), indentWidth, numIndents + 1, bulletWidth)
    );
  }

  private void append(TimeMeasurement round, String indent, int numIndents, String bullet, int firstColumnWidth, StringBuilder sb) {
    int originalLength = sb.length();
    for (int i = 0; i < numIndents; i++) {
      sb.append(indent);
    }
    sb.append(bullet).append(round.getId());
    while (sb.length() < originalLength + firstColumnWidth) sb.append(" ");
    sb.append(" - ").append(TimeMeasurement.formatDuration(round.getMillis())).append('\n');
    round.getRounds().forEach(subround -> this.append(subround, indent, numIndents + 1, bullet, firstColumnWidth, sb));
  }

  /**
   * Stops all {@link TimeMeasurement}s known to this instance.
   */
  public void stopAll() {
    this.rounds.values().forEach(TimeMeasurement::stop);
  }

  /**
   * Get the {@link Experiment} managed by this instance.
   *
   * @return the {@link Experiment}
   */
  public Experiment getExperiment() {
    return this.experiment;
  }
}
