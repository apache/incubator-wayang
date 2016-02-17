package org.qcri.rheem.spark.compiler;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.spark.operators.SparkFilterOperator;

import java.util.function.BinaryOperator;
import java.util.function.Predicate;

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
    public <I, O> Transformator<I, O> compile(TransformationDescriptor<I, O> descriptor) {
        return new Transformator<>(descriptor.getJavaImplementation());
    }

    /**
     * Compile a key extraction.
     *
     * @return a compiled function
     */
    public <T, K> KeyExtractor<T, K> compileToKeyExtractor(TransformationDescriptor<T, K> descriptor) {
        return new KeyExtractor<>(descriptor.getJavaImplementation());
    }


    public <I, O> FlatMapper<I, O> compile(FlatMapDescriptor<I, O> descriptor) {
        return new FlatMapper<>(descriptor.getJavaImplementation());

    }

    /**
     * Compile a reduction.
     *
     * @param descriptor describes the transformation
     * @param <Type>     input/output type of the transformation
     * @return a compiled function
     */
    public <Type> Reducer<Type> compile(ReduceDescriptor<Type> descriptor) {
        return new Reducer<>(descriptor.getJavaImplementation());
    }

    public <Type> FilterWrapper<Type> compile(PredicateDescriptor<Type> predicateDescriptor) {
        return new FilterWrapper<>(predicateDescriptor.getJavaImplementation());
    }

    /**
     * Spark function for transforming data quanta.
     */
    public static class Transformator<I, O> implements Function<I, O>, RheemSparkFunction {

        private final java.util.function.Function<I, O> impl;

        public Transformator(java.util.function.Function<I, O> impl) {
            this.impl = impl;
        }

        @Override
        public O call(I i) throws Exception {
            return this.impl.apply(i);
        }

        @Override
        public Object getRheemFunction() {
            return this.impl;
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
     * Spark function for transforming data quanta.
     */
    public static class FlatMapper<I, O> implements FlatMapFunction<I, O>, RheemSparkFunction {

        private final java.util.function.Function<I, Iterable<O>> impl;

        public FlatMapper(java.util.function.Function<I, Iterable<O>> impl) {
            this.impl = impl;
        }

        @Override
        public Iterable<O> call(I i) throws Exception {
            return this.impl.apply(i);
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


    public static class FilterWrapper<Type> implements Function<Type, Boolean>, RheemSparkFunction {

        private Predicate<Type> impl;

        public FilterWrapper(Predicate<Type> impl) {
            this.impl = impl;
        }

        @Override
        public Boolean call(Type el) throws Exception {
            return this.impl.test(el);
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
