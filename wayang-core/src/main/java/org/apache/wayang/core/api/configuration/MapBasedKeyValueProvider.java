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

import org.apache.commons.lang3.Validate;
import org.apache.wayang.core.api.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of {@link KeyValueProvider} that uses a {@link Map} to provide a value.
 */
public class MapBasedKeyValueProvider<Key, Value> extends KeyValueProvider<Key, Value> {

    private final Map<Key, Value> storedValues = new HashMap<>();

    private final boolean isCaching;

    /**
     * Creates a new caching instance.
     */
    public MapBasedKeyValueProvider(KeyValueProvider<Key, Value> parent) {
        this(parent, true);
    }

    /**
     * Creates a new caching instance.
     */
    public MapBasedKeyValueProvider(KeyValueProvider<Key, Value> parent, Configuration configuration) {
        this(parent, configuration, true);
    }

    /**
     * Creates a new instance.
     *
     * @param isCaching if, when a value is provided by the {@code parent}, that value should be cached in the new instance
     */
    public MapBasedKeyValueProvider(KeyValueProvider<Key, Value> parent, boolean isCaching) {
        super(parent);
        this.isCaching = isCaching;
    }


    /**
     * Creates a new instance.
     *
     * @param isCaching if, when a value is provided by the {@code parent}, that value should be cached in the new instance
     */
    public MapBasedKeyValueProvider(Configuration configuration, boolean isCaching) {
        super(null, configuration);
        this.isCaching = isCaching;
    }

    /**
     * Creates a new instance.
     */
    public MapBasedKeyValueProvider(KeyValueProvider<Key, Value> parent, Configuration configuration, boolean isCaching) {
        super(parent, configuration);
        this.isCaching = isCaching;
    }

    @Override
    public Value tryToProvide(Key key, KeyValueProvider<Key, Value> requestee) {
        Validate.notNull(key);
        return this.storedValues.get(key);
    }

    @Override
    protected void processParentEntry(Key key, Value value) {
        super.processParentEntry(key, value);
        if (this.isCaching && value != null) {
            this.storedValues.putIfAbsent(key, value);
        }
    }

    @Override
    public void set(Key key, Value value) {
        Validate.notNull(key);
        this.storedValues.put(key, value);
    }

}
