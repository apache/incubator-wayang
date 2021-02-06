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

package org.apache.wayang.core.api.configuration;

import org.apache.wayang.core.api.Configuration;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Implementation of {@link KeyValueProvider} that uses a {@link Function} to provide a value.
 */
public class FunctionalKeyValueProvider<Key, Value> extends KeyValueProvider<Key, Value> {

    private final BiFunction<Key, KeyValueProvider<Key, Value>, Value> providerFunction;

    public FunctionalKeyValueProvider(Function<Key, Value> providerFunction, Configuration configuration) {
        this(null, configuration, uncur(providerFunction));
    }

    public FunctionalKeyValueProvider(KeyValueProvider<Key, Value> parent, Function<Key, Value> providerFunction) {
        this(parent, parent.configuration, uncur(providerFunction));
    }

    public FunctionalKeyValueProvider(BiFunction<Key, KeyValueProvider<Key, Value>, Value> providerFunction,
                                      Configuration configuration) {
        this(null, configuration, providerFunction);
    }
    public FunctionalKeyValueProvider(KeyValueProvider<Key, Value> parent,
                                      BiFunction<Key, KeyValueProvider<Key, Value>, Value> providerFunction) {
        this(parent, parent.configuration, providerFunction);
    }

    private FunctionalKeyValueProvider(KeyValueProvider<Key, Value> parent,
                                      Configuration configuration,
                                      BiFunction<Key, KeyValueProvider<Key, Value>, Value> providerFunction) {
        super(parent, configuration);
        this.providerFunction = providerFunction;
    }

    private static <Key, Value> BiFunction<Key, KeyValueProvider<Key, Value>, Value> uncur(Function<Key, Value> function) {
        return (key, provider) -> function.apply(key);
    }

    @Override
    protected Value tryToProvide(Key key, KeyValueProvider<Key, Value> requestee) {
        return this.providerFunction.apply(key, requestee);
    }

}
