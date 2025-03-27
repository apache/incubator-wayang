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
package org.apache.wayang.flink.operators;

import org.apache.flink.util.SplittableIterator;
import java.util.Iterator;
import java.util.List;
import java.io.Serializable;

public class CollectionSplittableIterator<T> extends SplittableIterator<T> implements Serializable {
    //private transient final Iterator<T> iterator;
    private int numSplits;
    private int head;
    private final List<T> collection;

    public CollectionSplittableIterator(List<T> collection, int numSplits) {
        //this.iterator = collection.iterator();
        this.collection = collection;
        this.numSplits = numSplits;
    }

    @Override
    public Iterator<T>[] split(int numSplits) {
        // Split the collection into chunks
        int chunkSize = (int) Math.ceil((double) numElements() / this.numSplits);
        @SuppressWarnings("unchecked")
        Iterator<T>[] splits = new Iterator[this.numSplits];

        System.out.println("Splitting into "  + this.numSplits + " splits");

        for (int i = 0; i < this.numSplits; i++) {
            int fromIndex = i * chunkSize;
            int toIndex = Math.min(fromIndex + chunkSize, numElements());
            System.out.println("Splitting from "  + fromIndex + " to " + toIndex);
            splits[i] = new CollectionSplittableIterator<>(this.collection.subList(fromIndex, toIndex), 1);
        }

        return splits;
    }

    @Override
    public boolean hasNext() {
        return this.head < this.collection.size() - 1;
        //return iterator.hasNext();
    }

    @Override
    public T next() {
        //return iterator.next();
        T next = this.collection.get(this.head);
        this.head++;

        return next;
    }

    @Override
    public int getMaximumNumberOfSplits() {
        return this.numSplits;
    }

    private int numElements() {
        return this.collection.size();
    }
}

