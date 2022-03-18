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

package org.apache.wayang.spark.compiler;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.util.Iterators;
import org.apache.wayang.spark.execution.SparkExecutionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * Wraps a {@link Function} as a {@link FlatMapFunction}.
 */
public class ExtendedMapPartitionsFunctionAdapter<InputType, OutputType>
        implements FlatMapFunction<Iterator<InputType>, OutputType> {

    private final FunctionDescriptor.ExtendedSerializableFunction<Iterable<InputType>, Iterable<OutputType>> impl;

    private final SparkExecutionContext executionContext;

    public ExtendedMapPartitionsFunctionAdapter(
            FunctionDescriptor.ExtendedSerializableFunction<Iterable<InputType>, Iterable<OutputType>> extendedFunction,
            SparkExecutionContext sparkExecutionContext) {
        this.impl = extendedFunction;
        this.executionContext = sparkExecutionContext;
    }

    @Override
    public Iterator<OutputType> call(Iterator<InputType> it) throws Exception {
        this.impl.open(executionContext);
        List<OutputType> out = new ArrayList<>();
        while (it.hasNext()) {
            final Iterable<OutputType> mappedPartition = this.impl.apply(Iterators.wrapWithIterable(it));
            for (OutputType dataQuantum : mappedPartition) {
                out.add(dataQuantum);
            }
        }
        return out.iterator();
    }
}
