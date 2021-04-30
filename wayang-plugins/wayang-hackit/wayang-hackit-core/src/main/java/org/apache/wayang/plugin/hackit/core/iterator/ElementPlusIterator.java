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

/**
 * ElementPlusIterator extends from {@link Iterator}.
 *
 * ElementPlusIterator add one unique element to a know {@link Iterator}, this unique element
 * it will be consumed first.
 *
 * @param <T> type of the element of the iterator
 */
public class ElementPlusIterator<T> implements Iterator<T> {

    /**
     * Indicate if the first element it was consumed or not yet
     */
    private boolean element_consumed = false;

    /**
     * The element that was included to the iterator
     */
    private T element;

    /**
     * The iterator where the element was added
     */
    private Iterator<T> iterator;


    /**
     * Construct of ElementPlusIterator
     *
     * @param element is the object that will be consumed first
     * @param iterator iterator that will be consumed after the original element
     */
    public ElementPlusIterator(T element, Iterator<T> iterator) {
        if(element == null){
            throw new RuntimeException("the element can't be null");
        }
        this.element = element;
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        if( ! this.element_consumed ){
            return true;
        }
        return this.iterator.hasNext();
    }

    @Override
    public T next() {
        if( ! this.element_consumed ){
            this.element_consumed = true;
            return this.element;
        }
        return this.iterator.next();
    }
}
