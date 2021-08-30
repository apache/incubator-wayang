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

package profiledb.model;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Objects;

/**
 * An experiment comprises {@link Measurement}s from one specific {@link Subject} execution.
 */
public class Experiment {

    /**
     * Identifier for this instance.
     */
    private String id;

    /**
     * Description for this instance. (Optional)
     */
    private String description;

    /**
     * When this experiment has been started.
     */
    private long startTime;

    /**
     * Tags to group multiple Experiment instances. (Optional)
     */
    private Collection<String> tags;

    /**
     * {@link Measurement}s captured for this instance.
     */
    private Collection<Measurement> measurements;

    /**
     * The {@link Subject} being experimented with.
     */
    private Subject subject;

    /**
     * For deserialization.
     */
    private Experiment() {
    }

    /**
     * Create a new instance that is starting right now.
     *
     * @param id      Identifier for the new instance
     * @param subject the {@link Subject}
     * @param tags    tags to group several experiments
     */
    public Experiment(String id, Subject subject, String... tags) {
        this(id, subject, System.currentTimeMillis(), tags);
    }

    /**
     * Create a new instance.
     *
     * @param id        Identifier for the new instance
     * @param subject   the {@link Subject} of this experiment
     * @param startTime start timestamp of this experiment
     * @param tags      tags to group several experiments
     */
    public Experiment(String id, Subject subject, long startTime, String... tags) {
        this.id = id;
        this.subject = subject;
        this.startTime = startTime;
        this.tags = Arrays.asList(tags);
        this.measurements = new LinkedList<>();
    }

    /**
     * Adds a description for this instance.
     *
     * @param description the description
     * @return this instance
     */
    public Experiment withDescription(String description) {
        this.description = description;
        return this;
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public Collection<String> getTags() {
        return tags;
    }

    public void setTags(Collection<String> tags) {
        this.tags = tags;
    }

    public void addMeasurement(Measurement measurement) {
        this.measurements.add(measurement);
    }

    public Collection<Measurement> getMeasurements() {
        return measurements;
    }

    public Subject getSubject() {
        return this.subject;
    }

    public void setSubject(Subject subject) {
        this.subject = subject;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Experiment that = (Experiment) o;
        return startTime == that.startTime &&
                Objects.equals(id, that.id) &&
                Objects.equals(description, that.description) &&
                Objects.equals(tags, that.tags) &&
                Objects.equals(subject, that.subject);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, startTime);
    }

    @Override
    public String toString() {
        return String.format(
                "%s[%s, %d tags, %d measurements]",
                this.getClass().getSimpleName(),
                this.id,
                this.tags.size(),
                this.measurements.size()
        );
    }
}
