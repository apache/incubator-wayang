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

package org.apache.wayang.core.api.configuration;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * Used by {@link Configuration}s to provide some value.
 */
public abstract class ValueProvider<Value> {

    public static class NotAvailableException extends WayangException {
        public NotAvailableException(String message) {
            super(message);
        }
    }

    protected final Logger logger = LogManager.getLogger(this.getClass());

    private final Configuration configuration;

    protected final ValueProvider<Value> parent;

    private String warningSlf4j;

    protected ValueProvider(Configuration configuration, ValueProvider<Value> parent) {
        this.parent = parent;
        this.configuration = configuration;
    }

    public Value provide() {
        return this.provide(this);
    }

    protected Value provide(ValueProvider<Value> requestee) {
        if (this.warningSlf4j != null) {
            this.logger.warn(this.warningSlf4j);
        }

        // Look for a custom answer.
        Value value = this.tryProvide(requestee);
        if (value != null) {
            return value;
        }

        // If present, delegate request to parent.
        if (this.parent != null) {
            return this.parent.provide(requestee);
        }

        throw new NotAvailableException(String.format("Could not provide value."));
    }

    /**
     * Try to provide a value.
     *
     * @param requestee the original instance asked to provide a value
     * @return the requested value or {@code null} if the request could not be served
     */
    protected abstract Value tryProvide(ValueProvider<Value> requestee);

    public Optional<Value> optionallyProvide() {
        try {
            return Optional.of(this.provide());
        } catch (NotAvailableException nske) {
            return Optional.empty();
        }
    }

    public ValueProvider<Value> withSlf4jWarning(String message) {
        this.warningSlf4j = message;
        return this;
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }
}
