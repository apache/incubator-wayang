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
package org.apache.wayang.plugin.hackit.core.tagger.wrapper.template;

import java.util.Iterator;

/**
 * FlatMapTemplate is the template that provide the abstraction to work with Flatmap operations and also
 * allows to wrap some function made by the user.
 *
 * FlatMapTemplate generate as output a {@link Iterator} this could be an extension of {@link org.apache.wayang.plugin.hackit.core.iterator.HackitIterator}
 *
 * @param <I> Input type of the original Function
 * @param <O> Output type of the original function
 */
public interface FlatMapTemplate<I, O> {

    /**
     * Execute the logic over one element and generate as output a {@link Iterator}
     *
     * @param input element to transform
     * @return {@link Iterator} that contains the output's
     */
    public Iterator<O> execute(I input);
}
