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

package org.apache.wayang.commons.util.profiledb.measurement;

import org.apache.wayang.commons.util.profiledb.model.Measurement;
import org.apache.wayang.commons.util.profiledb.model.Type;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Objects;

/**
 * {@link Measurement} implementation for test purposes.
 */
@Type("test-time")
public class TestTimeMeasurement extends Measurement {

    private long millis;

    private Collection<Measurement> submeasurements;

    public TestTimeMeasurement(String id, long millis) {
        super(id);
        this.millis = millis;
        this.submeasurements = new LinkedList<>();
    }

    public long getMillis() {
        return millis;
    }

    public void setMillis(long millis) {
        this.millis = millis;
    }

    public Collection<Measurement> getSubmeasurements() {
        return submeasurements;
    }

    public void addSubmeasurements(TestTimeMeasurement submeasurements) {
        this.submeasurements.add(submeasurements);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        TestTimeMeasurement that = (TestTimeMeasurement) o;
        return millis == that.millis &&
                Objects.equals(submeasurements, that.submeasurements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), millis, submeasurements);
    }
}
