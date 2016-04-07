package org.qcri.rheem.profiler.spark;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.RheemArrays;
import org.qcri.rheem.profiler.data.DataGenerators;
import org.qcri.rheem.spark.operators.*;

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
    public static UnaryOperatorProfiler createSparkFlatMapProfiler() {
        return createSparkFlatMapProfiler(
                DataGenerators.createRandomIntegerSupplier(new Random(42)),
                RheemArrays::asList,
                Integer.class, Integer.class,
                new Configuration()
        );
    }


    /**
     * Creates a custom {@link SparkFlatMapOperator} profiler.
     */
    public static <In, Out> UnaryOperatorProfiler createSparkFlatMapProfiler(Supplier<In> dataGenerator,
                                                                             FunctionDescriptor.SerializableFunction<In, Iterable<Out>> udf,
                                                                             Class<In> inClass,
                                                                             Class<Out> outClass,
                                                                             Configuration configuration) {
        return new UnaryOperatorProfiler(
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
    public static UnaryOperatorProfiler createSparkMapProfiler() {
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
    public static <In, Out> UnaryOperatorProfiler createSparkMapProfiler(Supplier<In> dataGenerator,
                                                                         FunctionDescriptor.SerializableFunction<In, Out> udf,
                                                                         Class<In> inClass,
                                                                         Class<Out> outClass,
                                                                         Configuration configuration) {
        return new UnaryOperatorProfiler(
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
    public static UnaryOperatorProfiler createSparkFilterProfiler() {
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
    public static <Type> UnaryOperatorProfiler createSparkFilterProfiler(Supplier<Type> dataGenerator,
                                                                         PredicateDescriptor.SerializablePredicate<Type> udf,
                                                                         Class<Type> inOutClass,
                                                                         Configuration configuration) {
        return new UnaryOperatorProfiler(
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
    public static UnaryOperatorProfiler createSparkReduceByProfiler() {
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
    public static <In, Key> UnaryOperatorProfiler createSparkReduceByProfiler(Supplier<In> dataGenerator,
                                                                              FunctionDescriptor.SerializableFunction<In, Key> keyUdf,
                                                                              FunctionDescriptor.SerializableBinaryOperator<In> udf,
                                                                              Class<In> inOutClass,
                                                                              Class<Key> keyClass,
                                                                              Configuration configuration) {
        return new UnaryOperatorProfiler(
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
    public static UnaryOperatorProfiler createSparkGlobalReduceProfiler() {
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
    public static <Type> UnaryOperatorProfiler createSparkGlobalReduceProfiler(Supplier<Type> dataGenerator,
                                                                               FunctionDescriptor.SerializableBinaryOperator<Type> udf,
                                                                               Class<Type> inOutClass,
                                                                               Configuration configuration) {
        return new UnaryOperatorProfiler(
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
    public static UnaryOperatorProfiler createSparkDistinctProfiler() {
        return createSparkDistinctProfiler(
                DataGenerators.createReservoirBasedStringSupplier(new ArrayList<>(), 0.7, new Random(42), 4, 20),
                String.class,
                new Configuration()
        );
    }

    /**
     * Creates a custom {@link SparkGlobalReduceOperator} profiler.
     */
    public static <Type> UnaryOperatorProfiler createSparkDistinctProfiler(Supplier<Type> dataGenerator,
                                                                           Class<Type> inOutClass,
                                                                           Configuration configuration) {
        return new UnaryOperatorProfiler(
                () -> new SparkDistinctOperator<>(DataSetType.createDefault(inOutClass)),
                configuration,
                dataGenerator
        );
    }

    /**
     * Creates a default {@link SparkSortOperator} profiler.
     */
    public static UnaryOperatorProfiler createSparkSortProfiler() {
        return createSparkSortProfiler(
                DataGenerators.createReservoirBasedStringSupplier(new ArrayList<>(), 0.7, new Random(42), 4, 20),
                String.class,
                new Configuration()
        );
    }

    /**
     * Creates a custom {@link SparkSortOperator} profiler.
     */
    public static <Type> UnaryOperatorProfiler createSparkSortProfiler(Supplier<Type> dataGenerator,
                                                                       Class<Type> inOutClass,
                                                                       Configuration configuration) {
        return new UnaryOperatorProfiler(
                () -> new SparkSortOperator<>(DataSetType.createDefault(inOutClass)),
                configuration,
                dataGenerator
        );
    }

    /**
     * Creates a default {@link SparkCountOperator} profiler.
     */
    public static UnaryOperatorProfiler createSparkCountOperator() {
        return createSparkCountOperator(
                DataGenerators.createReservoirBasedStringSupplier(new ArrayList<>(), 0.7, new Random(42), 4, 20),
                String.class,
                new Configuration()
        );
    }

    /**
     * Creates a custom {@link SparkCountOperator} profiler.
     */
    public static <In> UnaryOperatorProfiler createSparkCountOperator(Supplier<In> dataGenerator,
                                                                       Class<In> inClass,
                                                                       Configuration configuration) {
        return new UnaryOperatorProfiler(
                () -> new SparkCountOperator<>(DataSetType.createDefault(inClass)),
                configuration,
                dataGenerator
        );
    }

}
