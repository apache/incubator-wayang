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

package org.apache.wayang.profiler.spark;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.spark.operators.SparkCollectionSource;
import org.apache.wayang.spark.operators.SparkTextFileSource;

import java.util.ArrayList;
import java.util.function.Supplier;

/**
 * {@link SparkOperatorProfiler} for the {@link SparkTextFileSource}.
 */
public class SparkCollectionSourceProfiler extends SparkSourceProfiler {

    private final ArrayList<Object> collection;

    public <T extends Object> SparkCollectionSourceProfiler(Configuration configuration,
                                             Supplier<T> dataQuantumGenerator,
                                             DataSetType<T> outputType) {
        this(new ArrayList<>(), configuration, dataQuantumGenerator, outputType);
    }

    private <T extends Object> SparkCollectionSourceProfiler(ArrayList<T> collection,
                                                             Configuration configuration,
                                                             Supplier<T> dataQuantumGenerator,
                                                             DataSetType<T> outputType) {
        super(() -> new SparkCollectionSource<>(collection, outputType), configuration, dataQuantumGenerator);
        this.collection = (ArrayList<Object>) collection;
    }

    @Override
    protected void prepareInput(int inputIndex, long inputCardinality) {
        assert inputIndex == 0;
        assert inputCardinality <= Integer.MAX_VALUE;

        this.collection.clear();
        this.collection.ensureCapacity((int) inputCardinality);
        final Supplier<?> supplier = this.dataQuantumGenerators.get(0);
        for (long i = 0; i < inputCardinality; i++) {
            this.collection.add(supplier.get());
        }
    }

    @Override
    public void cleanUp() {
        super.cleanUp();

        this.collection.clear();
        this.collection.trimToSize();
    }
}
