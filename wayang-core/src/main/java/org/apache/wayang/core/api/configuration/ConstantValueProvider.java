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

/**
 * Used by {@link Configuration}s to provide some value.
 */
public class ConstantValueProvider<Value> extends ValueProvider<Value> {

    private Value value;

    public ConstantValueProvider(Value value, Configuration configuration) {
        this(configuration, null);
        this.setValue(value);
    }

    public ConstantValueProvider(Configuration configuration) {
        this(configuration, null);
    }

    public ConstantValueProvider(ValueProvider<Value> parent) {
        this(parent.getConfiguration(), parent);
    }

    public ConstantValueProvider(Configuration configuration, ValueProvider<Value> parent) {
        super(configuration, parent);
    }

    public void setValue(Value value) {
        this.value = value;
    }

    @Override
    protected Value tryProvide(ValueProvider<Value> requestee) {
        return this.value;
    }

}
