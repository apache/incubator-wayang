package org.qcri.rheem.java.profiler;

import org.qcri.rheem.core.function.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.RheemArrays;
import org.qcri.rheem.java.operators.*;

import java.util.*;
import java.util.function.Supplier;

/**
 * Utilities to create {@link OperatorProfiler} instances.
 */
public class OperatorProfilers {

    public static JavaTextFileSourceProfiler createJavaTextFileSource() {
        return new JavaTextFileSourceProfiler(DataGenerators.createRandomStringSupplier(20, 40, new Random(42)));
    }

    public static UnaryOperatorProfiler createJavaMapProfiler() {
        return createJavaMapProfiler(
                DataGenerators.createRandomIntegerSupplier(new Random(42)),
                i -> i,
                Integer.class, Integer.class
        );
    }

    public static <In, Out> UnaryOperatorProfiler createJavaMapProfiler(Supplier<In> dataGenerator,
                                                                        FunctionDescriptor.SerializableFunction<In, Out> udf,
                                                                        Class<In> inClass,
                                                                        Class<Out> outClass) {
        return new UnaryOperatorProfiler(
                () -> new JavaMapOperator<>(
                        DataSetType.createDefault(inClass),
                        DataSetType.createDefault(outClass),
                        new TransformationDescriptor<>(udf, inClass, outClass)
                ),
                dataGenerator
        );
    }

    public static UnaryOperatorProfiler createJavaFlatMapProfiler() {
        final Random random = new Random(42);
        return new UnaryOperatorProfiler(
                () -> new JavaFlatMapOperator<>(
                        DataSetType.createDefault(Integer.class),
                        DataSetType.createDefault(Integer.class),
                        new FlatMapDescriptor<>(
                                RheemArrays::asList,
                                Integer.class,
                                Integer.class
                        )
                ),
                random::nextInt
        );
    }

    public static <In, Out> UnaryOperatorProfiler createJavaFlatMapProfiler(Supplier<In> dataGenerator,
                                                                            FunctionDescriptor.SerializableFunction<In, Iterable<Out>> udf,
                                                                            Class<In> inClass,
                                                                            Class<Out> outClass) {
        return new UnaryOperatorProfiler(
                () -> new JavaFlatMapOperator<>(
                        DataSetType.createDefault(inClass),
                        DataSetType.createDefault(outClass),
                        new FlatMapDescriptor<>(udf, inClass, outClass)
                ),
                dataGenerator
        );
    }

    public static UnaryOperatorProfiler createJavaFilterProfiler() {
        final Random random = new Random(42);
        return new UnaryOperatorProfiler(
                () -> new JavaFilterOperator<>(
                        DataSetType.createDefault(Integer.class),
                        new PredicateDescriptor<>(i -> (i & 1) == 0, Integer.class)
                ),
                random::nextInt
        );
    }


    public static UnaryOperatorProfiler createJavaReduceByProfiler() {
        return createJavaReduceByProfiler(
                DataGenerators.createReservoirBasedStringSupplier(new ArrayList<>(), 0.7, new Random(42), 4, 20),
                String::new,
                (s1, s2) -> s1,
                String.class,
                String.class
        );
    }

    public static <In, Key> UnaryOperatorProfiler createJavaReduceByProfiler(Supplier<In> dataGenerator,
                                                                             FunctionDescriptor.SerializableFunction<In, Key> keyUdf,
                                                                             FunctionDescriptor.SerializableBinaryOperator<In> udf,
                                                                             Class<In> inOutClass,
                                                                             Class<Key> keyClass) {
        return new UnaryOperatorProfiler(
                () -> new JavaReduceByOperator<>(
                        DataSetType.createDefault(inOutClass),
                        new TransformationDescriptor<>(keyUdf, inOutClass, keyClass),
                        new ReduceDescriptor<>(udf, inOutClass)
                ),
                dataGenerator
        );
    }

    public static BinaryOperatorProfiler createJavaJoinProfiler() {
        final List<String> stringReservoir = new ArrayList<>();
        final double reuseProbability = 0.3;
        final Random random = new Random(42);
        final int minLen = 4, maxLen = 6;
        Supplier<String> reservoirStringSupplier = DataGenerators.createReservoirBasedStringSupplier(stringReservoir, reuseProbability, random, minLen, maxLen);
        return new BinaryOperatorProfiler(
                () -> new JavaJoinOperator<>(
                        DataSetType.createDefault(String.class),
                        DataSetType.createDefault(String.class),
                        new TransformationDescriptor<>(
                                String::new,
                                String.class,
                                String.class
                        ),
                        new TransformationDescriptor<>(
                                String::new,
                                String.class,
                                String.class
                        )
                ),
                reservoirStringSupplier,
                reservoirStringSupplier
        );
    }

    public static BinaryOperatorProfiler createJavaUnionProfiler() {
        final List<String> stringReservoir = new ArrayList<>();
        final double reuseProbability = 0.3;
        final Random random = new Random(42);
        Supplier<String> reservoirStringSupplier = DataGenerators.createReservoirBasedStringSupplier(stringReservoir, reuseProbability, random, 4, 6);
        return new BinaryOperatorProfiler(
                () -> new JavaUnionAllOperator<>(DataSetType.createDefault(String.class)),
                reservoirStringSupplier,
                reservoirStringSupplier
        );
    }

    public static SinkProfiler createJavaLocalCallbackSinkProfiler() {
        return new SinkProfiler(
                () -> new JavaLocalCallbackSink<>(obj -> {
                }, DataSetType.createDefault(Integer.class)),
                DataGenerators.createRandomIntegerSupplier(new Random(42))
        );
    }

    public static <T> SinkProfiler createCollectingJavaLocalCallbackSinkProfiler() {
        Collection<T> collector = new LinkedList<>();
        return new SinkProfiler(
                () -> new JavaLocalCallbackSink<>(collector::add, DataSetType.createDefault(Integer.class)),
                DataGenerators.createRandomIntegerSupplier(new Random(42))
        );
    }

}
