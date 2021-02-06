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

import java.util.Collection;
import java.util.HashSet;
import java.util.function.Function;

/**
 * {@link CollectionProvider} implementation based on a blacklist and a whitelist.
 */
public class FunctionalCollectionProvider<Value> extends CollectionProvider<Value> {

    private final Function<Configuration, Collection<Value>> providerFunction;

    public FunctionalCollectionProvider(Function<Configuration, Collection<Value>> providerFunction, Configuration configuration) {
        super(configuration);
        this.providerFunction = providerFunction;
    }

    public FunctionalCollectionProvider(Function<Configuration, Collection<Value>> providerFunction,
                                        Configuration configuration,
                                        CollectionProvider parent) {
        super(configuration, parent);
        this.providerFunction = providerFunction;
    }

    @Override
    public Collection<Value> provideAll(Configuration configuration) {
        Collection<Value> values = this.parent == null ? new HashSet<>() : new HashSet<>(this.parent.provideAll(configuration));
        values.addAll(this.providerFunction.apply(configuration));
        return values;
    }

}
