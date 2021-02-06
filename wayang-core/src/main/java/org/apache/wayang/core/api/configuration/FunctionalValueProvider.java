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

import java.util.function.Function;

/**
 * Used by {@link Configuration}s to provide some value.
 */
public class FunctionalValueProvider<Value> extends ValueProvider<Value> {

    private final Function<ValueProvider<Value>, Value> valueFunction;

    public FunctionalValueProvider(Function<ValueProvider<Value>, Value> valueFunction, Configuration configuration) {
        this(valueFunction, configuration, null);
    }

    public FunctionalValueProvider(Function<ValueProvider<Value>, Value> valueFunction, ValueProvider<Value> parent) {
        this(valueFunction, parent.getConfiguration(), parent);
    }

    public FunctionalValueProvider(Function<ValueProvider<Value>, Value> valueFunction, Configuration configuration, ValueProvider<Value> parent) {
        super(configuration, parent);
        this.valueFunction = valueFunction;
    }

    @Override
    protected Value tryProvide(ValueProvider<Value> requestee) {
        return this.valueFunction.apply(requestee);
    }

}
