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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * This utility maintains canonical sets of objects.
 */
public class Canonicalizer<T> implements Set<T> {

    /**
     * Contains the objects.
     */
    private final Map<T, T> entries;

    public Canonicalizer(int numExpectedEntries) {
        this.entries = new HashMap<T, T>(numExpectedEntries);
    }

    public Canonicalizer() {
        this(32);
    }

    public Canonicalizer(Iterable<? extends T> objs) {
        this();
        this.addAll(objs);
    }

    public Canonicalizer(Collection<? extends T> objs) {
        this(objs.size());
        this.addAll(objs);
    }

    public Canonicalizer(T... objs) {
        this(objs.length);
        for (T obj : objs) {
            this.add(obj);
        }
    }


    /**
     * Add the given element if it is not contained in this instance, yet.
     *
     * @param obj the element to be added potentially
     * @return {@code obj} if it was added, otherwise the existing element
     */
    public T getOrAdd(T obj) {
        final T existingObj = this.entries.putIfAbsent(obj, obj);
        return existingObj == null ? obj : existingObj;
    }

    public void addAll(Iterable<? extends T> objs) {
        objs.forEach(this::getOrAdd);
    }

    @Override
    public int size() {
        return this.entries.size();
    }

    @Override
    public boolean isEmpty() {
        return this.entries.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return this.entries.containsKey(o);
    }

    @Override
    public Iterator<T> iterator() {
        return this.entries.keySet().iterator();
    }

    @Override
    public Object[] toArray() {
        return this.entries.keySet().toArray();
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        return this.entries.keySet().toArray(a);
    }

    @Override
    public boolean add(T t) {
        return this.entries.putIfAbsent(t, t) == null;
    }

    @Override
    public boolean remove(Object o) {
        return this.entries.remove(o) != null;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return this.entries.keySet().containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return c.stream().map(this::add).reduce(false, Boolean::logicalOr);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new RuntimeException("#retainAll() is not implemented");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return this.removeAll(c);
    }

    @Override
    public void clear() {
        this.entries.clear();
    }

    @Override
    public String toString() {
        return this.entries.keySet().toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        Canonicalizer<?> that = (Canonicalizer<?>) o;
        return Objects.equals(this.entries, that.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.entries);
    }
}
