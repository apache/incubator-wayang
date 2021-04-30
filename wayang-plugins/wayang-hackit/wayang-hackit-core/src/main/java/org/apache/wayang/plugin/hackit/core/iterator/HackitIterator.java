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

import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.util.Iterator;
import java.util.function.Function;

/**
 * HackitIterator extends {@link FunctionIterator} and provides an signature for the conversion of
 * the tuples into a {@link HackitTuple} during they process.
 *
 * @param <K> type of the key on the {@link HackitTuple}
 * @param <T> type of the element that it contains the {@link HackitTuple}
 */
public class HackitIterator<K, T> extends FunctionIterator<T, HackitTuple<K, T>>{

    /**
     * default construct
     * @param base {@link Iterator} that element will be wrapper inside of a {@link HackitTuple}
     * @param function it a {@link Function} that convert the the {@link HackitTuple}
     */
    public HackitIterator(Iterator<T> base, Function<T, HackitTuple<K, T>> function) {
        super(base, function);
    }
}
