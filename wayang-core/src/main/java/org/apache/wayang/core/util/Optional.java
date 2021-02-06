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

package org.apache.wayang.core.util;

import java.util.stream.Stream;

/**
 * Utility similar to {@link java.util.Optional}. However, it supports {@code null} as a valid value.
 */
public abstract class Optional<T> {

    /**
     * @return whether this instance contains a value.
     */
    public abstract boolean isAvailable();

    /**
     * @return the value contained by this instance
     * @throws IllegalStateException if {@link #isAvailable()} is {@code false}
     */
    public abstract T getValue();

    /**
     * @return a {@link Stream} containing the optional value
     */
    public abstract Stream<T> stream();

    private static final Optional NA = new Optional() {
        @Override
        public boolean isAvailable() {
            return false;
        }

        @Override
        public Object getValue() {
            throw new IllegalStateException("No value available.");
        }

        @Override
        public Stream stream() {
            return Stream.empty();
        }
    };

    /**
     * Create a n/a instance.
     *
     * @return an {@link Optional} without a value
     */
    @SuppressWarnings("unchecked")
    public static <T> Optional<T> na() {
        return NA;
    }

    /**
     * Create a new instance with the given {@code value}.
     *
     * @param value the value of the new {@link Optional}
     * @return the new instance
     */
    public static <T> Optional<T> of(final T value) {
        return new Optional<T>() {
            @Override
            public boolean isAvailable() {
                return true;
            }

            @Override
            public T getValue() {
                return value;
            }

            @Override
            public Stream<T> stream() {
                return Stream.of(value);
            }
        };
    }
}
