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

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.wayang.core.function.ConsumerDescriptor;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.MapPartitionsDescriptor;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.flink.compiler.criterion.WayangConvergenceCriterion;
import org.apache.wayang.flink.execution.FlinkExecutionContext;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A compiler translates Wayang functions into executable Flink functions.
 */
public class FunctionCompiler {

    /**
     * Compile a transformation.
     *
     * @param descriptor describes the transformation
     * @param <I>        input type of the transformation
     * @param <O>        output type of the transformation
     * @return a compiled function
     */
    public <I, O> MapFunction<I, O> compile(TransformationDescriptor<I, O> descriptor) {
        // This is a dummy method but shows the intention of having something compilable in the descriptors.
        Function<I, O> function = descriptor.getJavaImplementation();
        return (MapFunction<I, O>) i -> function.apply(i);
    }

    /**
     * Compile a transformation.
     *
     * @param flatMapDescriptor describes the transformation
     * @param <I>        input type of the transformation
     * @param <O>        output type of the transformation
     * @return a compiled function
     */
    public <I, O> FlatMapFunction<I, O> compile(FunctionDescriptor.SerializableFunction<I, Iterable<O>> flatMapDescriptor) {
        return (t, collector) -> flatMapDescriptor.apply(t).forEach(collector::collect);
    }

    /**
     * Compile a reduction.
     *
     * @param descriptor describes the transformation
     * @param <T>     input/output type of the transformation
     * @return a compiled function
     */
    public <T> ReduceFunction<T> compile(ReduceDescriptor<T> descriptor) {
        // This is a dummy method but shows the intention of having something compilable in the descriptors.
        BiFunction<T, T, T> reduce_function = descriptor.getJavaImplementation();
        return new ReduceFunction<T>() {
            @Override
            public T reduce(T t, T t1) throws Exception {
                return reduce_function.apply(t, t1);
            }
        };
    }

    public <T> FilterFunction<T> compile(PredicateDescriptor.SerializablePredicate<T> predicateDescriptor) {
        return t -> predicateDescriptor.test(t);
    }


    public <T> OutputFormat<T> compile(ConsumerDescriptor.SerializableConsumer<T> consumerDescriptor) {
        return new OutputFormatConsumer<T>(consumerDescriptor);
    }


    public <T, K> KeySelector<T, K> compileKeySelector(TransformationDescriptor<T, K> descriptor){
        return new KeySelectorFunction<T, K>(descriptor);
    }

    public <T0, T1, O> CoGroupFunction<T0, T1, O> compileCoGroup(){
        return new FlinkCoGroupFunction<T0, T1, O>();
    }


    public <T> TextOutputFormat.TextFormatter<T> compileOutput(TransformationDescriptor<T, String> formattingDescriptor) {
        Function<T, String> format = formattingDescriptor.getJavaImplementation();
        return new TextOutputFormat.TextFormatter<T>(){

            @Override
            public String format(T value) {
                return format.apply(value);
            }
        };
    }

    /**
     * Compile a partition transformation.
     *
     * @param descriptor describes the transformation
     * @param <I>        input type of the transformation
     * @param <O>        output type of the transformation
     * @return a compiled function
     */
    public <I, O> MapPartitionFunction<I, O> compile(MapPartitionsDescriptor<I, O> descriptor){
        Function<Iterable<I>, Iterable<O>> function = descriptor.getJavaImplementation();
        return new MapPartitionFunction<I, O>() {
            @Override
            public void mapPartition(Iterable<I> iterable, Collector<O> collector) throws Exception {
                Iterable<O> out = function.apply(iterable);
                for(O element: out){
                    collector.collect(element);
                }

            }
        };
    }

    public <T> WayangConvergenceCriterion compile(PredicateDescriptor<Collection<T>> descriptor){
        FunctionDescriptor.SerializablePredicate<Collection<T>> predicate = descriptor.getJavaImplementation();
        return new WayangConvergenceCriterion(predicate);
    }


    public <I, O> RichFlatMapFunction<I, O> compile(FunctionDescriptor.ExtendedSerializableFunction<I, Iterable<O>> flatMapDescriptor, FlinkExecutionContext exe) {

        return new RichFlatMapFunction<I, O>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                flatMapDescriptor.open(exe);
            }

            @Override
            public void flatMap(I value, Collector<O> out) throws Exception {
                flatMapDescriptor.apply(value).forEach(out::collect);
            }
        };
    }


    public <I, O> RichMapFunction<I, O> compile(TransformationDescriptor<I, O> mapDescriptor, FlinkExecutionContext fex ) {

        FunctionDescriptor.ExtendedSerializableFunction<I, O> map = (FunctionDescriptor.ExtendedSerializableFunction) mapDescriptor.getJavaImplementation();
        return new RichMapFunction<I, O>() {
            @Override
            public O map(I value) throws Exception {
                return map.apply(value);
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                map.open(fex);
            }
        };
    }



    public <I, O> RichMapPartitionFunction<I, O> compile(MapPartitionsDescriptor<I, O> descriptor, FlinkExecutionContext fex){
        FunctionDescriptor.ExtendedSerializableFunction<Iterable<I>, Iterable<O>> function =
                (FunctionDescriptor.ExtendedSerializableFunction<Iterable<I>, Iterable<O>>)
                        descriptor.getJavaImplementation();
        return new RichMapPartitionFunction<I, O>() {
            @Override
            public void mapPartition(Iterable<I> iterable, Collector<O> collector) throws Exception {
                function.apply(iterable).forEach(
                        element -> {
                            collector.collect(element);
                        }
                );

            }
            @Override
            public void open(Configuration parameters) throws Exception {
                function.open(fex);
            }
        };
    }
}
