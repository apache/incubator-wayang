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

package org.apache.wayang.commons.util.profiledb.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The subject of an {@link Experiment}, e.g., an application or algorithm.
 */
public class Subject {

    /**
     * Identifier for the subject.
     */
    private String id;

    /**
     * Version of the subject.
     */
    private String version;

    /**
     * Configuration of this object.
     */
    private Map<String, Object> configuration = new HashMap<>();

    /**
     * Creates a new instance.
     *
     * @param id      Identifier for the subject
     * @param version To distinguish different versions among instances with the same {@code id}
     */
    public Subject(String id, String version) {
        this.id = id;
        this.version = version;
    }

    /**
     * Adds a configuration.
     *
     * @param key   Key of the configuration entry
     * @param value Value for the new configuration entry; must be JSON-compatible, e.g. {@link Integer} or {@link String}
     * @return this instance
     */
    public Subject addConfiguration(String key, Object value) {
        this.configuration.put(key, value);
        return this;
    }

    @Override
    public String toString() {
        return String.format("%s[%s:%s]", this.getClass().getSimpleName(), this.id, this.version);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Subject subject = (Subject) o;
        return Objects.equals(id, subject.id) &&
                Objects.equals(version, subject.version) &&
                Objects.equals(configuration, subject.configuration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, version, configuration);
    }
}
