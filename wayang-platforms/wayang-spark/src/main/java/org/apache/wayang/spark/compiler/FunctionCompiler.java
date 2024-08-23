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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.wayang.core.function.FlatMapDescriptor;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.MapPartitionsDescriptor;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.spark.execution.SparkExecutionContext;
import org.apache.wayang.spark.operators.SparkExecutionOperator;

import java.util.Iterator;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;
import java.io.Serializable;

/**
 * A compiler translates Wayang functions into executable Java functions.
 */
public class FunctionCompiler {

    /**
     * Create an appropriate {@link Function} for deploying the given {@link TransformationDescriptor}
     * on Apache Spark.
     *
     * @param descriptor      describes the transformation function
     * @param operator        that executes the {@link Function}; only required if the {@code descriptor} describes an {@link ExtendedFunction}
     * @param operatorContext contains optimization information for the {@code operator}
     * @param inputs          that feed the {@code operator}; only required if the {@code descriptor} describes an {@link ExtendedFunction}
     */
    public <I, O> Function<I, O> compile(TransformationDescriptor<I, O> descriptor,
                                         SparkExecutionOperator operator,
                                         OptimizationContext.OperatorContext operatorContext,
                                         ChannelInstance[] inputs) {
        final java.util.function.Function<I, O> javaImplementation = descriptor.getJavaImplementation();
        if (javaImplementation instanceof FunctionDescriptor.ExtendedSerializableFunction) {
            return new ExtendedMapFunctionAdapter<>(
                    (FunctionDescriptor.ExtendedSerializableFunction<I, O>) javaImplementation,
                    new SparkExecutionContext(operator, inputs, operatorContext.getOptimizationContext().getIterationNumber())
            );
        } else {
            return new MapFunctionAdapter<>(javaImplementation);
        }
    }

    /**
     * Create an appropriate {@link Function} for deploying the given {@link MapPartitionsDescriptor}
     * on Apache Spark's {@link JavaRDD#mapPartitions(FlatMapFunction)}.
     *
     * @param descriptor      describes the function
     * @param operator        that executes the {@link Function}; only required if the {@code descriptor} describes an {@link ExtendedFunction}
     * @param operatorContext contains optimization information for the {@code operator}
     * @param inputs          that feed the {@code operator}; only required if the {@code descriptor} describes an {@link ExtendedFunction}
     */
    public <I, O> FlatMapFunction<Iterator<I>, O> compile(MapPartitionsDescriptor<I, O> descriptor,
                                                          SparkExecutionOperator operator,
                                                          OptimizationContext.OperatorContext operatorContext,
                                                          ChannelInstance[] inputs) {
        final java.util.function.Function<Iterable<I>, Iterable<O>> javaImplementation = descriptor.getJavaImplementation();

        if (javaImplementation instanceof FunctionDescriptor.ExtendedSerializableFunction) {
            return new ExtendedMapPartitionsFunctionAdapter<>(
                    (FunctionDescriptor.ExtendedSerializableFunction<Iterable<I>, Iterable<O>>) javaImplementation,
                    new SparkExecutionContext(operator, inputs, operatorContext.getOptimizationContext().getIterationNumber())
            );
        } else {
            return new MapPartitionsFunctionAdapter<>(javaImplementation);
        }
    }

    /**
     * Compile a key extraction.
     *
     * @return a compiled function
     */
    public <T, K> KeyExtractor<T, K> compileToKeyExtractor(TransformationDescriptor<T, K> descriptor) {
        return new KeyExtractor<>(descriptor.getJavaImplementation());
    }


    /**
     * Create an appropriate {@link FlatMapFunction} for deploying the given {@link FlatMapDescriptor}
     * on Apache Spark.
     *
     * @param descriptor      describes the function
     * @param operator        that executes the {@link Function}; only required if the {@code descriptor} describes an {@link ExtendedFunction}
     * @param operatorContext contains optimization information for the {@code operator}
     * @param inputs          that feed the {@code operator}; only required if the {@code descriptor} describes an {@link ExtendedFunction}
     */
    public <I, O> FlatMapFunction<I, O> compile(FlatMapDescriptor<I, O> descriptor,
                                                SparkExecutionOperator operator,
                                                OptimizationContext.OperatorContext operatorContext,
                                                ChannelInstance[] inputs) {
        final java.util.function.Function<I, Iterable<O>> javaImplementation = descriptor.getJavaImplementation();
        if (javaImplementation instanceof FunctionDescriptor.ExtendedSerializableFunction) {
            return new ExtendedFlatMapFunctionAdapter<>(
                    (FunctionDescriptor.ExtendedSerializableFunction<I, Iterable<O>>) javaImplementation,
                    new SparkExecutionContext(operator, inputs, operatorContext.getOptimizationContext().getIterationNumber())
            );
        } else {
            return new FlatMapFunctionAdapter<>(javaImplementation);
        }
    }

    /**
     * Create an appropriate {@link Function} for deploying the given {@link ReduceDescriptor}
     * on Apache Spark.
     */
    public <T> Function2<T, T, T> compile(ReduceDescriptor<T> descriptor,
                                          SparkExecutionOperator operator,
                                          OptimizationContext.OperatorContext operatorContext,
                                          ChannelInstance[] inputs) {
        final BinaryOperator<T> javaImplementation = descriptor.getJavaImplementation();
        if (javaImplementation instanceof FunctionDescriptor.ExtendedSerializableBinaryOperator) {
            return new ExtendedBinaryOperatorAdapter<>(
                    (FunctionDescriptor.ExtendedSerializableBinaryOperator<T>) javaImplementation,
                    new SparkExecutionContext(operator, inputs, operatorContext.getOptimizationContext().getIterationNumber())
            );
        } else {
            return new BinaryOperatorAdapter<>(javaImplementation);
        }
    }

    /**
     * Create an appropriate {@link Function}-based predicate for deploying the given {@link PredicateDescriptor}
     * on Apache Spark.
     *
     * @param predicateDescriptor describes the function
     * @param operator            that executes the {@link Function}; only required if the {@code descriptor} describes an {@link ExtendedFunction}
     * @param operatorContext     contains optimization information for the {@code operator}
     * @param inputs              that feed the {@code operator}; only required if the {@code descriptor} describes an {@link ExtendedFunction}
     */
    public <Type> Function<Type, Boolean> compile(
            PredicateDescriptor<Type> predicateDescriptor,
            SparkExecutionOperator operator,
            OptimizationContext.OperatorContext operatorContext,
            ChannelInstance[] inputs) {
        final Predicate<Type> javaImplementation = predicateDescriptor.getJavaImplementation();
        if (javaImplementation instanceof PredicateDescriptor.ExtendedSerializablePredicate) {
            return new ExtendedPredicateAdapater<>(
                    (PredicateDescriptor.ExtendedSerializablePredicate<Type>) javaImplementation,
                    new SparkExecutionContext(operator, inputs, operatorContext.getOptimizationContext().getIterationNumber())
            );
        } else {
            return new PredicateAdapter<>(javaImplementation);
        }
    }


    /**
     * Spark function for building pair RDDs.
     */
    public static class KeyExtractor<T, K> implements PairFunction<T, K, T>, WayangSparkFunction {

        private final java.util.function.Function<T, K> impl;

        public KeyExtractor(java.util.function.Function<T, K> impl) {
            this.impl = impl;
        }

        @Override
        public scala.Tuple2<K, T> call(T t) throws Exception {
            K key = this.impl.apply(t);
            return new scala.Tuple2<>(key, t);
        }

        @Override
        public Object getWayangFunction() {
            return this.impl;
        }
    }


    /**
     * Spark function for aggregating data quanta.
     */
    public static class Reducer<Type> implements Function2<Type, Type, Type>, WayangSparkFunction {

        private final BinaryOperator<Type> impl;

        public Reducer(BinaryOperator<Type> impl) {
            this.impl = impl;
        }

        @Override
        public Type call(Type i0, Type i1) throws Exception {
            return this.impl.apply(i0, i1);
        }

        @Override
        public Object getWayangFunction() {
            return this.impl;
        }
    }


    /**
     * Describes functions coming from Wayang, designated for Spark.
     */
    public interface WayangSparkFunction {

        /**
         * @return the original code object as has been defined in the Wayang API
         */
        Object getWayangFunction();

    }
}
