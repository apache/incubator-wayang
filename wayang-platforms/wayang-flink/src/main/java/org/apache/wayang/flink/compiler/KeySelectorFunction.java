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

package org.apache.wayang.flink.compiler;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.wayang.core.function.TransformationDescriptor;

import java.util.function.Function;

/**
 * Wrapper for {@link KeySelector}
 */
public class KeySelectorFunction<T, K> implements KeySelector<T, K>, ResultTypeQueryable<K> {

    public Function<T, K> impl;

    public Class<K> key;

    public TypeInformation<K> typeInformation;

    public KeySelectorFunction(TransformationDescriptor<T, K> transformationDescriptor) {

        this.impl = transformationDescriptor.getJavaImplementation();
        this.key  = transformationDescriptor.getOutputType().getTypeClass();
        this.typeInformation = TypeInformation.of(this.key);
    }

    public K getKey(T object){
            return this.impl.apply(object);
        }

    @Override
    public TypeInformation getProducedType() {
        return this.typeInformation;
    }
}
