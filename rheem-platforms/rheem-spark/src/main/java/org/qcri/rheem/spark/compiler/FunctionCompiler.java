package org.qcri.rheem.spark.compiler;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;

import java.util.function.BinaryOperator;

/**
 * A compiler translates Rheem functions into executable Java functions.
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
    public <I, O> Function<I, O> compile(TransformationDescriptor<I, O> descriptor) {
        return new Transformator<>(descriptor.getJavaImplementation());
    }

    /**
     * Compile a key extraction.
     *
     * @return a compiled function
     */
    public <T, K> PairFunction<T, K, T> compileToKeyExtractor(TransformationDescriptor<T, K> descriptor) {
        return new KeyExtractor<>(descriptor.getJavaImplementation());
    }


    public <I, O> FlatMapFunction<I, O> compile(FlatMapDescriptor<I, O> descriptor) {
        return new FlatMapper<>(descriptor.getJavaImplementation());

    }

    /**
     * Compile a reduction.
     *
     * @param descriptor describes the transformation
     * @param <Type>     input/output type of the transformation
     * @return a compiled function
     */
    public <Type> Function2<Type, Type, Type> compile(ReduceDescriptor<Type> descriptor) {
        return new Reducer<>(descriptor.getJavaImplementation());
    }

    /**
     * Spark function for transforming data quanta.
     */
    private static class Transformator<I, O> implements Function<I, O> {

        private final java.util.function.Function<I, O> impl;

        public Transformator(java.util.function.Function<I, O> impl) {
            this.impl = impl;
        }

        @Override
        public O call(I i) throws Exception {
            return this.impl.apply(i);
        }
    }


    /**
     * Spark function for building pair RDDs.
     */
    private static class KeyExtractor<T, K> implements PairFunction<T, K, T> {

        private final java.util.function.Function<T, K> impl;

        public KeyExtractor(java.util.function.Function<T, K> impl) {
            this.impl = impl;
        }

        @Override
        public scala.Tuple2<K, T> call(T t) throws Exception {
            K key = this.impl.apply(t);
            return new scala.Tuple2<>(key, t);
        }
    }

    /**
     * Spark function for transforming data quanta.
     */
    private static class FlatMapper<I, O> implements FlatMapFunction<I, O> {

        private final java.util.function.Function<I, Iterable<O>> impl;

        public FlatMapper(java.util.function.Function<I, Iterable<O>> impl) {
            this.impl = impl;
        }

        @Override
        public Iterable<O> call(I i) throws Exception {
            return this.impl.apply(i);
        }
    }

    /**
     * Spark function for aggregating data quanta.
     */
    private static class Reducer<Type> implements Function2<Type, Type, Type> {

        private final BinaryOperator<Type> impl;

        public Reducer(BinaryOperator<Type> impl) {
            this.impl = impl;
        }

        @Override
        public Type call(Type i0, Type i1) throws Exception {
            return this.impl.apply(i0, i1);
        }
    }
}
