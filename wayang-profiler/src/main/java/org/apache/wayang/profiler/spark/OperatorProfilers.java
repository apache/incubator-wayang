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
import org.apache.wayang.core.function.FlatMapDescriptor;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.WayangArrays;
import org.apache.wayang.profiler.data.DataGenerators;
import org.apache.wayang.spark.operators.SparkCartesianOperator;
import org.apache.wayang.spark.operators.SparkCollectionSource;
import org.apache.wayang.spark.operators.SparkCountOperator;
import org.apache.wayang.spark.operators.SparkDistinctOperator;
import org.apache.wayang.spark.operators.SparkFilterOperator;
import org.apache.wayang.spark.operators.SparkFlatMapOperator;
import org.apache.wayang.spark.operators.SparkGlobalReduceOperator;
import org.apache.wayang.spark.operators.SparkJoinOperator;
import org.apache.wayang.spark.operators.SparkLocalCallbackSink;
import org.apache.wayang.spark.operators.SparkMapOperator;
import org.apache.wayang.spark.operators.SparkMaterializedGroupByOperator;
import org.apache.wayang.spark.operators.SparkReduceByOperator;
import org.apache.wayang.spark.operators.SparkSortOperator;
import org.apache.wayang.spark.operators.SparkTextFileSource;
import org.apache.wayang.spark.operators.SparkUnionAllOperator;

import java.util.ArrayList;
import java.util.Random;
import java.util.function.Supplier;

/**
 * Utilities to create {@link SparkOperatorProfiler} instances.
 */
public class OperatorProfilers {

    /**
     * Create a default {@link SparkTextFileSource} profiler.
     */
    public static SparkTextFileSourceProfiler createSparkTextFileSourceProfiler() {
        return createSparkTextFileSourceProfiler(
                DataGenerators.createRandomStringSupplier(20, 40, new Random(42)),
                new Configuration()
        );
    }

    /**
     * Create a custom {@link SparkTextFileSource} profiler.
     */
    public static SparkTextFileSourceProfiler createSparkTextFileSourceProfiler(Supplier<String> dataGenerator,
                                                                                Configuration configuration) {
        return new SparkTextFileSourceProfiler(configuration, dataGenerator);
    }

    /**
     * Create a default {@link SparkCollectionSource} profiler.
     */
    public static SparkTextFileSourceProfiler createSparkCollectionSourceProfiler() {
        return createSparkCollectionSourceProfiler(
                DataGenerators.createRandomStringSupplier(20, 40, new Random(42)),
                new Configuration()
        );
    }

    /**
     * Create a custom {@link SparkTextFileSource} profiler.
     */
    public static SparkTextFileSourceProfiler createSparkCollectionSourceProfiler(Supplier<String> dataGenerator,
                                                                                  Configuration configuration) {
        return new SparkTextFileSourceProfiler(configuration, dataGenerator);
    }

    /**
     * Creates a default {@link SparkFlatMapOperator} profiler.
     */
    public static SparkUnaryOperatorProfiler createSparkFlatMapProfiler() {
        return createSparkFlatMapProfiler(
                DataGenerators.createRandomIntegerSupplier(new Random(42)),
                WayangArrays::asList,
                Integer.class, Integer.class,
                new Configuration()
        );
    }


    /**
     * Creates a custom {@link SparkFlatMapOperator} profiler.
     */
    public static <In, Out> SparkUnaryOperatorProfiler createSparkFlatMapProfiler(Supplier<In> dataGenerator,
                                                                                  FunctionDescriptor.SerializableFunction<In, Iterable<Out>> udf,
                                                                                  Class<In> inClass,
                                                                                  Class<Out> outClass,
                                                                                  Configuration configuration) {
        return new SparkUnaryOperatorProfiler(
                () -> new SparkFlatMapOperator<>(
                        DataSetType.createDefault(inClass),
                        DataSetType.createGrouped(outClass),
                        new FlatMapDescriptor<>(udf, inClass, outClass)
                ),
                configuration,
                dataGenerator
        );
    }

    /**
     * Creates a default {@link SparkMapOperator} profiler.
     */
    public static SparkUnaryOperatorProfiler createSparkMapProfiler() {
        return createSparkMapProfiler(
                DataGenerators.createRandomIntegerSupplier(new Random(42)),
                i -> i,
                Integer.class, Integer.class,
                new Configuration()
        );
    }


    /**
     * Creates a custom {@link SparkMapOperator} profiler.
     */
    public static <In, Out> SparkUnaryOperatorProfiler createSparkMapProfiler(Supplier<In> dataGenerator,
                                                                              FunctionDescriptor.SerializableFunction<In, Out> udf,
                                                                              Class<In> inClass,
                                                                              Class<Out> outClass,
                                                                              Configuration configuration) {
        return new SparkUnaryOperatorProfiler(
                () -> new SparkMapOperator<>(
                        DataSetType.createDefault(inClass),
                        DataSetType.createDefault(outClass),
                        new TransformationDescriptor<>(udf, inClass, outClass)
                ),
                configuration,
                dataGenerator
        );
    }

    /**
     * Creates a default {@link SparkFilterOperator} profiler.
     */
    public static SparkUnaryOperatorProfiler createSparkFilterProfiler() {
        return createSparkFilterProfiler(
                DataGenerators.createRandomIntegerSupplier(new Random(42)),
                i -> true,
                Integer.class,
                new Configuration()
        );
    }


    /**
     * Creates a custom {@link SparkMapOperator} profiler.
     */
    public static <Type> SparkUnaryOperatorProfiler createSparkFilterProfiler(Supplier<Type> dataGenerator,
                                                                              PredicateDescriptor.SerializablePredicate<Type> udf,
                                                                              Class<Type> inOutClass,
                                                                              Configuration configuration) {
        return new SparkUnaryOperatorProfiler(
                () -> new SparkFilterOperator<>(
                        DataSetType.createDefault(inOutClass),
                        new PredicateDescriptor<>(udf, inOutClass)
                ),
                configuration,
                dataGenerator
        );
    }


    /**
     * Creates a default {@link SparkReduceByOperator} profiler.
     */
    public static SparkUnaryOperatorProfiler createSparkReduceByProfiler() {
        return createSparkReduceByProfiler(
                DataGenerators.createReservoirBasedStringSupplier(new ArrayList<>(), 0.7, new Random(42), 4, 20),
                String::new,
                (s1, s2) -> s1,
                String.class,
                String.class,
                new Configuration()
        );
    }

    /**
     * Creates a custom {@link SparkReduceByOperator} profiler.
     */
    public static <In, Key> SparkUnaryOperatorProfiler createSparkReduceByProfiler(Supplier<In> dataGenerator,
                                                                                   FunctionDescriptor.SerializableFunction<In, Key> keyUdf,
                                                                                   FunctionDescriptor.SerializableBinaryOperator<In> udf,
                                                                                   Class<In> inOutClass,
                                                                                   Class<Key> keyClass,
                                                                                   Configuration configuration) {
        return new SparkUnaryOperatorProfiler(
                () -> new SparkReduceByOperator<>(
                        DataSetType.createDefault(inOutClass),
                        new TransformationDescriptor<>(keyUdf, inOutClass, keyClass),
                        new ReduceDescriptor<>(udf, inOutClass)
                ),
                configuration,
                dataGenerator
        );
    }

    /**
     * Creates a default {@link SparkGlobalReduceOperator} profiler.
     */
    public static SparkUnaryOperatorProfiler createSparkGlobalReduceProfiler() {
        return createSparkGlobalReduceProfiler(
                DataGenerators.createReservoirBasedStringSupplier(new ArrayList<>(), 0.7, new Random(42), 4, 20),
                (s1, s2) -> s1,
                String.class,
                new Configuration()
        );
    }

    /**
     * Creates a custom {@link SparkGlobalReduceOperator} profiler.
     */
    public static <Type> SparkUnaryOperatorProfiler createSparkGlobalReduceProfiler(Supplier<Type> dataGenerator,
                                                                                    FunctionDescriptor.SerializableBinaryOperator<Type> udf,
                                                                                    Class<Type> inOutClass,
                                                                                    Configuration configuration) {
        return new SparkUnaryOperatorProfiler(
                () -> new SparkGlobalReduceOperator<>(
                        DataSetType.createDefault(inOutClass),
                        new ReduceDescriptor<>(udf, inOutClass)
                ),
                configuration,
                dataGenerator
        );
    }

    /**
     * Creates a default {@link SparkDistinctOperator} profiler.
     */
    public static SparkUnaryOperatorProfiler createSparkDistinctProfiler() {
        return createSparkDistinctProfiler(
                DataGenerators.createReservoirBasedStringSupplier(new ArrayList<>(), 0.7, new Random(42), 4, 20),
                String.class,
                new Configuration()
        );
    }

    /**
     * Creates a custom {@link SparkGlobalReduceOperator} profiler.
     */
    public static <Type> SparkUnaryOperatorProfiler createSparkDistinctProfiler(Supplier<Type> dataGenerator,
                                                                                Class<Type> inOutClass,
                                                                                Configuration configuration) {
        return new SparkUnaryOperatorProfiler(
                () -> new SparkDistinctOperator<>(DataSetType.createDefault(inOutClass)),
                configuration,
                dataGenerator
        );
    }

    /**
     * Creates a default {@link SparkSortOperator} profiler.
     */
    public static SparkUnaryOperatorProfiler createSparkSortProfiler() {
        return createSparkSortProfiler(
                DataGenerators.createReservoirBasedStringSupplier(new ArrayList<>(), 0.7, new Random(42), 4, 20),
                String.class,
                new Configuration()
        );
    }

    /**
     * Creates a custom {@link SparkSortOperator} profiler.
     */
    public static <Type> SparkUnaryOperatorProfiler createSparkSortProfiler(Supplier<Type> dataGenerator,
                                                                            Class<Type> inOutClass,
                                                                            Configuration configuration) {
        return new SparkUnaryOperatorProfiler(
                () -> new SparkSortOperator<>(new TransformationDescriptor<>(in->in, inOutClass, inOutClass),
                        DataSetType.createDefault(inOutClass)),
                configuration,
                dataGenerator
        );
    }

    /**
     * Creates a default {@link SparkCountOperator} profiler.
     */
    public static SparkUnaryOperatorProfiler createSparkCountProfiler() {
        return createSparkCountProfiler(
                DataGenerators.createReservoirBasedStringSupplier(new ArrayList<>(), 0.7, new Random(42), 4, 20),
                String.class,
                new Configuration()
        );
    }

    /**
     * Creates a custom {@link SparkCountOperator} profiler.
     */
    public static <In> SparkUnaryOperatorProfiler createSparkCountProfiler(Supplier<In> dataGenerator,
                                                                           Class<In> inClass,
                                                                           Configuration configuration) {
        return new SparkUnaryOperatorProfiler(
                () -> new SparkCountOperator<>(DataSetType.createDefault(inClass)),
                configuration,
                dataGenerator
        );
    }

    /**
     * Creates a default {@link SparkMaterializedGroupByOperator} profiler.
     */
    public static SparkUnaryOperatorProfiler createSparkMaterializedGroupByProfiler() {
        return createSparkMaterializedGroupByProfiler(
                DataGenerators.createReservoirBasedStringSupplier(new ArrayList<>(), 0.7, new Random(42), 4, 20),
                String::new,
                String.class,
                String.class,
                new Configuration()
        );
    }

    /**
     * Creates a custom {@link SparkMaterializedGroupByOperator} profiler.
     */
    public static <In, Key> SparkUnaryOperatorProfiler createSparkMaterializedGroupByProfiler(Supplier<In> dataGenerator,
                                                                                              FunctionDescriptor.SerializableFunction<In, Key> keyUdf,
                                                                                              Class<In> inClass,
                                                                                              Class<Key> keyClass,
                                                                                              Configuration configuration) {
        return new SparkUnaryOperatorProfiler(
                () -> new SparkMaterializedGroupByOperator<>(
                        new TransformationDescriptor<>(keyUdf, inClass, keyClass),
                        DataSetType.createDefault(inClass),
                        DataSetType.createGrouped(inClass)
                ),
                configuration,
                dataGenerator
        );
    }

    /**
     * Creates a default {@link SparkJoinOperator} profiler.
     */
    public static BinaryOperatorProfiler createSparkJoinProfiler() {
        // NB: If we generate the Strings from within Spark, we will have two different reservoirs for each input.
        final DataGenerators.Generator<String> stringGenerator = DataGenerators.createReservoirBasedStringSupplier(new ArrayList<>(), 0.7, new Random(42), 4, 20);
        return createSparkJoinProfiler(
                stringGenerator, String.class, String::new,
                stringGenerator, String.class, String::new,
                String.class, new Configuration()
        );
    }

    /**
     * Creates a custom {@link SparkJoinOperator} profiler.
     */
    public static <In0, In1, Key> BinaryOperatorProfiler createSparkJoinProfiler(
            Supplier<In0> dataGenerator0,
            Class<In0> inClass0,
            FunctionDescriptor.SerializableFunction<In0, Key> keyUdf0,
            Supplier<In1> dataGenerator1,
            Class<In1> inClass1,
            FunctionDescriptor.SerializableFunction<In1, Key> keyUdf1,
            Class<Key> keyClass,
            Configuration configuration) {
        return new BinaryOperatorProfiler(
                () -> new SparkJoinOperator<>(
                        DataSetType.createDefault(inClass0),
                        DataSetType.createDefault(inClass1),
                        new TransformationDescriptor<>(keyUdf0, inClass0, keyClass),
                        new TransformationDescriptor<>(keyUdf1, inClass1, keyClass)
                ),
                configuration,
                dataGenerator0,
                dataGenerator1
        );
    }

    /**
     * Creates a default {@link SparkUnionAllOperator} profiler.
     */
    public static BinaryOperatorProfiler createSparkUnionProfiler() {
        return createSparkUnionProfiler(
                DataGenerators.createRandomIntegerSupplier(new Random(42)),
                DataGenerators.createRandomIntegerSupplier(new Random(23)),
                Integer.class, new Configuration()
        );
    }

    /**
     * Creates a custom {@link SparkUnionAllOperator} profiler.
     */
    public static <Type> BinaryOperatorProfiler createSparkUnionProfiler(
            Supplier<Type> dataGenerator0,
            Supplier<Type> dataGenerator1,
            Class<Type> typeClass,
            Configuration configuration) {
        return new BinaryOperatorProfiler(
                () -> new SparkUnionAllOperator<>(DataSetType.createDefault(typeClass)),
                configuration,
                dataGenerator0,
                dataGenerator1
        );
    }

    /**
     * Creates a default {@link SparkCartesianOperator} profiler.
     */
    public static BinaryOperatorProfiler createSparkCartesianProfiler() {
        return createSparkCartesianProfiler(
                DataGenerators.createRandomIntegerSupplier(new Random(42)),
                DataGenerators.createRandomIntegerSupplier(new Random(23)),
                Integer.class, Integer.class, new Configuration()
        );
    }

    /**
     * Creates a custom {@link SparkCartesianOperator} profiler.
     */
    public static <In0, In1> BinaryOperatorProfiler createSparkCartesianProfiler(
            Supplier<In0> dataGenerator0,
            Supplier<In1> dataGenerator1,
            Class<In0> inClass0,
            Class<In1> inClass1,
            Configuration configuration) {
        return new BinaryOperatorProfiler(
                () -> new SparkCartesianOperator<>(DataSetType.createDefault(inClass0), DataSetType.createDefault(inClass1)),
                configuration,
                dataGenerator0,
                dataGenerator1
        );
    }

    /**
     * Creates a default {@link SparkLocalCallbackSink} profiler.
     */
    public static SinkProfiler createSparkLocalCallbackSinkProfiler() {
        return createSparkLocalCallbackSinkProfiler(
                DataGenerators.createRandomIntegerSupplier(new Random(42)),
                Integer.class,
                new Configuration()
        );
    }

    /**
     * Creates a custom {@link SparkLocalCallbackSink} profiler.
     */
    public static <Type> SinkProfiler createSparkLocalCallbackSinkProfiler(
            Supplier<Type> dataGenerator,
            Class<Type> typeClass,
            Configuration configuration) {
        return new SinkProfiler(
                () -> new SparkLocalCallbackSink<>(dataQuantum -> { }, DataSetType.createDefault(typeClass)),
                configuration,
                dataGenerator
        );
    }


}
