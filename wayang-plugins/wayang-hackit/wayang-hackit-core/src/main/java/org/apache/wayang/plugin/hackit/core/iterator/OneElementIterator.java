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
package org.apache.wayang.plugin.hackit.core.iterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class OneElementIterator<T> implements Iterator<T>, Iterable<T>{

    private final boolean removeAllowed;
    private boolean beforeFirst;
    private boolean removed;
    private T object;

    public OneElementIterator(T object) {
        this(object, true);
    }

    public OneElementIterator(T object, boolean removeAllowed) {
        this.beforeFirst = true;
        this.removed = false;
        this.object = object;
        this.removeAllowed = removeAllowed;
    }

    public boolean hasNext() {
        return this.beforeFirst && !this.removed;
    }

    public T next() {
        if (this.beforeFirst && !this.removed) {
            this.beforeFirst = false;
            return this.object;
        } else {
            throw new NoSuchElementException();
        }
    }

    public void remove() {
        if (this.removeAllowed) {
            if (!this.removed && !this.beforeFirst) {
                this.object = null;
                this.removed = true;
            } else {
                throw new IllegalStateException();
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public void reset() {
        this.beforeFirst = true;
    }

    @Override
    public Iterator<T> iterator() {
        return this;
    }
}
