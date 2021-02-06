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

package org.apache.wayang.profiler.java;

import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.operators.JavaCollectionSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Supplier;

/**
 * {@link OperatorProfiler} for {@link JavaCollectionSource}s.
 */
public class JavaCollectionSourceProfiler extends SourceProfiler {

    private Collection<Object> sourceCollection;

    public JavaCollectionSourceProfiler(Supplier<?> dataQuantumGenerator) {
        super(null, dataQuantumGenerator);
        this.operatorGenerator = this::createOperator; // We can only pass the method reference here.
    }

    private JavaCollectionSource createOperator() {
        final Object exampleDataQuantum = this.dataQuantumGenerators.get(0).get();
        return new JavaCollectionSource(this.sourceCollection, DataSetType.createDefault(exampleDataQuantum.getClass()));
    }


    @Override
    void setUpSourceData(long cardinality) throws Exception {
        // Create the #sourceCollection.
        final Supplier<?> dataQuantumGenerator = this.dataQuantumGenerators.get(0);
        this.sourceCollection = new ArrayList<>((int) cardinality);
        for (int i = 0; i < cardinality; i++) {
            this.sourceCollection.add(dataQuantumGenerator.get());
        }
    }

}
