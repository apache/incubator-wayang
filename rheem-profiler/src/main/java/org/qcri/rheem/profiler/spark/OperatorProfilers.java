package org.qcri.rheem.profiler.spark;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.profiler.data.DataGenerators;
import org.qcri.rheem.profiler.java.JavaTextFileSourceProfiler;
import org.qcri.rheem.spark.operators.SparkMapOperator;
import org.qcri.rheem.spark.operators.SparkReduceByOperator;
import org.qcri.rheem.spark.operators.SparkTextFileSource;

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
    public static <In, Out> org.qcri.rheem.profiler.spark.UnaryOperatorProfiler createSparkMapProfiler(Supplier<In> dataGenerator,
                                                                                                       FunctionDescriptor.SerializableFunction<In, Out> udf,
                                                                                                       Class<In> inClass,
                                                                                                       Class<Out> outClass,
                                                                                                       Configuration configuration) {
        return new org.qcri.rheem.profiler.spark.UnaryOperatorProfiler(
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

}
