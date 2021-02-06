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

import java.util.Iterator;
import java.util.function.Predicate;

/**
 * Utilities for the work with {@link Iterator}s.
 */
public class Iterators {

    /**
     * Test the a given {@link Predicate} on all remaining elements of an {@link Iterator} and return whether it always
     * evaluated to {@code true}.
     *
     * @param iterator     that should be inspected
     * @param predicate    that should be applied to the elements of the {@code iterator}
     * @param isAbortEarly whether search can be aborted on the first sight of {@code false}
     * @param <T>          type of the elements
     * @return whether all tests returned {@code true}
     */
    public static <T> boolean allMatch(Iterator<T> iterator, Predicate<T> predicate, boolean isAbortEarly) {
        boolean result = true;
        while (iterator.hasNext()) {
            result &= predicate.test(iterator.next());
            if (!result && isAbortEarly) {
                return false;
            }
        }
        return result;
    }

    /**
     * Wrap the given {@link Iterator} in an {@link Iterable}.
     *
     * @param iterator the {@link Iterator}
     * @return the {@link Iterable}
     */
    public static <T> Iterable<T> wrapWithIterable(Iterator<T> iterator) {
        return () -> iterator;
    }
}
