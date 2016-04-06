package org.qcri.rheem.profiler.spark;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.profiler.data.DataGenerators;
import org.qcri.rheem.spark.operators.SparkMapOperator;

import java.util.Random;
import java.util.function.Supplier;

/**
 * Utilities to create {@link SparkOperatorProfiler} instances.
 */
public class OperatorProfilers {

    public static UnaryOperatorProfiler createSparkMapProfiler() {
        return createSparkMapProfiler(
                DataGenerators.createRandomIntegerSupplier(new Random(42)),
                i -> i,
                Integer.class, Integer.class
        );
    }

    public static <In, Out> org.qcri.rheem.profiler.spark.UnaryOperatorProfiler createSparkMapProfiler(Supplier<In> dataGenerator,
                                                                        FunctionDescriptor.SerializableFunction<In, Out> udf,
                                                                        Class<In> inClass,
                                                                        Class<Out> outClass) {
        return new org.qcri.rheem.profiler.spark.UnaryOperatorProfiler(
                () -> new SparkMapOperator<>(
                        DataSetType.createDefault(inClass),
                        DataSetType.createDefault(outClass),
                        new TransformationDescriptor<>(udf, inClass, outClass)
                ),
                new Configuration(),
                dataGenerator
        );
    }

}
