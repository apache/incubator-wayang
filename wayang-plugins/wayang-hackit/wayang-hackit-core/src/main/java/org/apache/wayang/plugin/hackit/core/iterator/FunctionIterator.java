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

import java.io.Serializable;
import java.util.Iterator;
import java.util.function.Function;

/**
 * FunctionIterator implements {@link Iterator} and {@link Serializable}, because the function could be
 * serialized to be sent to several places at runtime
 *
 * FunctionIterator provides the option of converting the data in the iterator using an {@link Function},
 * this will transform the elements to a new kind given by param <O>
 *
 * @param <I> Type before of the transformation in the iterator
 * @param <O> Type after the transformation, this could be the same to <I>
 */
public class FunctionIterator<I, O> implements Iterator<O>, Serializable {

    /**
     * base is an {@link Iterator} that will be transformed during runtime
     */
    private Iterator<I> base;

    /**
     * function is a {@link Function} that will convert the element inside of <code>base</code>
     */
    private Function<I, O> function;

    /**
     * Constructor of FunctionIterator
     *
     * @param base {@link Iterator} that will be transformed at the time of consumption
     * @param function {@link Function} that will convert the data inside of <code>base</code>
     */
    public FunctionIterator(Iterator<I> base, Function<I, O> function) {
        this.base = base;
        this.function = function;
    }

    @Override
    public boolean hasNext() {
        return this.base.hasNext();
    }

    @Override
    public O next() {
        return this.function.apply(this.base.next());
    }
}
