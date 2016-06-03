package org.qcri.rheem.spark.compiler;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.qcri.rheem.core.function.*;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.spark.execution.SparkExecutionContext;
import org.qcri.rheem.spark.operators.SparkExecutionOperator;

import java.util.function.BinaryOperator;
import java.util.function.Predicate;

/**
 * A compiler translates Rheem functions into executable Java functions.
 */
public class FunctionCompiler {

    /**
     * Create an appropriate {@link Function} for deploying the given {@link TransformationDescriptor}
     * on Apache Spark.
     */
    public <I, O> Function<I, O> compile(TransformationDescriptor<I, O> descriptor,
                                         SparkExecutionOperator operator,
                                         ChannelInstance[] inputs) {
        final java.util.function.Function<I, O> javaImplementation = descriptor.getJavaImplementation();
        if (javaImplementation instanceof FunctionDescriptor.ExtendedSerializableFunction) {
            return new ExtendedFunctionAdapter<>(
                    (FunctionDescriptor.ExtendedSerializableFunction<I, O>) javaImplementation,
                    new SparkExecutionContext(operator, inputs)
            );
        } else {
            return new FunctionAdapter<>(javaImplementation);
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
     */
    public <I, O> FlatMapFunction<I, O> compile(FlatMapDescriptor<I, O> descriptor,
                                                SparkExecutionOperator operator,
                                                ChannelInstance[] inputs) {
        final java.util.function.Function<I, Iterable<O>> javaImplementation = descriptor.getJavaImplementation();
        if (javaImplementation instanceof FunctionDescriptor.ExtendedSerializableFunction) {
            return new ExtendedFunctionAdapter2<>(
                    (FunctionDescriptor.ExtendedSerializableFunction<I, Iterable<O>>) javaImplementation,
                    new SparkExecutionContext(operator, inputs)
            );
        } else {
            return new FunctionAdapter2<>(javaImplementation);
        }
    }

    /**
     * Create an appropriate {@link Function} for deploying the given {@link ReduceDescriptor}
     * on Apache Spark.
     */
    public <T> Function2<T, T, T> compile(ReduceDescriptor<T> descriptor,
                                          SparkExecutionOperator operator,
                                          ChannelInstance[] inputs) {
        final BinaryOperator<T> javaImplementation = descriptor.getJavaImplementation();
        if (javaImplementation instanceof FunctionDescriptor.ExtendedSerializableBinaryOperator) {
            return new ExtendedBinaryOperatorAdapter<>(
                    (FunctionDescriptor.ExtendedSerializableBinaryOperator<T>) javaImplementation,
                    new SparkExecutionContext(operator, inputs)
            );
        } else {
            return new BinaryOperatorAdapter<>(javaImplementation);
        }
    }

    /**
     * Create an appropriate {@link Function}-based predicate for deploying the given {@link PredicateDescriptor}
     * on Apache Spark.
     */
    public <Type> Function<Type, Boolean> compile(
            PredicateDescriptor<Type> predicateDescriptor,
            SparkExecutionOperator operator,
            ChannelInstance[] inputs) {
        final Predicate<Type> javaImplementation = predicateDescriptor.getJavaImplementation();
        if (javaImplementation instanceof PredicateDescriptor.ExtendedSerializablePredicate) {
            return new ExtendedPredicateAdapater<>(
                    (PredicateDescriptor.ExtendedSerializablePredicate<Type>) javaImplementation,
                    new SparkExecutionContext(operator, inputs)
            );
        } else {
            return new PredicateAdapter<>(javaImplementation);
        }
    }


    /**
     * Spark function for building pair RDDs.
     */
    public static class KeyExtractor<T, K> implements PairFunction<T, K, T>, RheemSparkFunction {

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
        public Object getRheemFunction() {
            return this.impl;
        }
    }


    /**
     * Spark function for aggregating data quanta.
     */
    public static class Reducer<Type> implements Function2<Type, Type, Type>, RheemSparkFunction {

        private final BinaryOperator<Type> impl;

        public Reducer(BinaryOperator<Type> impl) {
            this.impl = impl;
        }

        @Override
        public Type call(Type i0, Type i1) throws Exception {
            return this.impl.apply(i0, i1);
        }

        @Override
        public Object getRheemFunction() {
            return this.impl;
        }
    }


    /**
     * Describes functions coming from Rheem, designated for Spark.
     */
    public interface RheemSparkFunction {

        /**
         * @return the original code object as has been defined in the Rheem API
         */
        Object getRheemFunction();

    }
}
