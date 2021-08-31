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

import java.util.Objects;

/**
 * {@link Measurement} implementation for test purposes.
 */
@Type("test-mem")
public class TestMemoryMeasurement extends Measurement {

    private long timestamp;

    private long usedMb;

    public TestMemoryMeasurement(String id, long timestamp, long usedMb) {
        super(id);
        this.timestamp = timestamp;
        this.usedMb = usedMb;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getUsedMb() {
        return usedMb;
    }

    public void setUsedMb(long usedMb) {
        this.usedMb = usedMb;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        TestMemoryMeasurement that = (TestMemoryMeasurement) o;
        return timestamp == that.timestamp &&
                usedMb == that.usedMb;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), timestamp, usedMb);
    }

    @Override
    public String toString() {
        return "TestMemoryMeasurement{" +
                "timestamp=" + timestamp +
                ", usedMb=" + usedMb +
                '}';
    }
}
