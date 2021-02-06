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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * This utility helps to count elements.
 */
public class Counter<T> implements Iterable<Map.Entry<T, Integer>> {

    private final Map<T, Integer> counts = new HashMap<>();

    public int get(T element) {
        return this.counts.getOrDefault(element, 0);
    }

    /**
     * Counts the given {@code delta} for the given {@code element}.
     * @return the new count
     */
    public int add(T element, int delta) {
        final int currentCount = this.get(element);
        final int newCount = currentCount + delta;
        if (newCount == 0) {
            this.counts.remove(element);
        } else {
            this.counts.put(element, newCount);
        }

        return newCount;
    }

    public int increment(T element) {
        return this.add(element, 1);
    }

    public int decrement(T element) {
        return this.add(element, -1);
    }

    public void addAll(Counter<T> that) {
        this.addAll(that.counts);
    }

    private void addAll(Map<T, Integer> counts) {
        counts.entrySet().forEach(count -> this.add(count.getKey(), count.getValue()));
    }

    public boolean isEmpty() {
        return this.counts.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        Counter<?> counter = (Counter<?>) o;
        return Objects.equals(this.counts, counter.counts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.counts);
    }

    @Override
    public Iterator<Map.Entry<T, Integer>> iterator() {
        return this.counts.entrySet().iterator();
    }

    public void remove(T element) {
        this.counts.remove(element);
    }

    public void clear() {
        this.counts.clear();
    }
}
