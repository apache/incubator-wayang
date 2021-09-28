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

/**
 * OneElementIterator implements {@link Iterator} and {@link Iterable}
 *
 * OneElementIterator will contain just one element, but that element need to be treated as
 * an Iterator, this class allows handle all the functionalities that are related to that.
 *
 * @param <T> type of the element
 */
public class OneElementIterator<T> implements Iterator<T>, Iterable<T>{

    /**
     * Indicates if the element could be removed
     */
    private final boolean removeAllowed;

    /**
     * Indicates whether the process has started or not
     */
    private boolean beforeFirst;

    /**
     * Allows to follow the process as the object didn't exist
     */
    private boolean removed;

    /**
     * It is the element that will be processed at iteration time
     */
    private T object;

    /**
     * Default Constructor
     *
     * @param object element that will be processed
     */
    public OneElementIterator(T object) {
        this(object, true);
    }

    /**
     * Constructor that allows indicating the option to remove the object
     *
     * @param object element that will be processed
     * @param removeAllowed indicates if the element could be removed
     */
    public OneElementIterator(T object, boolean removeAllowed) {
        this.beforeFirst = true;
        this.removed = false;
        this.object = object;
        this.removeAllowed = removeAllowed;
    }

    @Override
    public boolean hasNext() {
        return this.beforeFirst && !this.removed;
    }

    @Override
    public T next() {
        if (this.beforeFirst && !this.removed) {
            this.beforeFirst = false;
            return this.object;
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
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

    /**
     * reset the Iterator if is needed, because the iterator it just one element, then is possible to do several
     * full iteration on top of the element.
     */
    public void reset() {
        this.beforeFirst = true;
    }

    @Override
    public Iterator<T> iterator() {
        return this;
    }
}
