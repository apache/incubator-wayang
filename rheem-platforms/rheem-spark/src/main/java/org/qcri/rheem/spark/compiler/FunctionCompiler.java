package org.qcri.rheem.spark.compiler;

import org.apache.spark.api.java.function.FlatMapFunction;

import org.apache.spark.api.java.function.PairFunction;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.function.KeyExtractorDescriptor;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;


import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Iterator;

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
        return new Function<I, O>() {
            @Override
            public O call(I i) throws Exception {
                return descriptor.getJavaImplementation().apply(i);
            }
        };
    }

    /**
     * Compile a key extraction.
     * @return a compiled function
     */
    public <T,K> PairFunction <T, K, T> compile(KeyExtractorDescriptor<T, K> descriptor) {
        return new PairFunction<T, K, T>() {
            @Override
            public Tuple2<K, T> call(T t) throws Exception {
                K key = descriptor.getJavaImplementation().apply(t);
                return new Tuple2<>(key, t);
            }
        };
    }


    public <I, O> FlatMapFunction<I, O> compile(FlatMapDescriptor<I, Iterator<O>> descriptor) {
        return new FlatMapFunction<I, O>() {
            @Override
            public Iterable<O> call(I i) throws Exception {
                Iterator <O> sourceIterator =  descriptor.getJavaImplementation().apply(i);
                Iterable<O> iterable = () -> sourceIterator;
                return iterable;
            }
        };

    }

    /**
     * Compile a reduction.
     *
     * @param descriptor describes the transformation
     * @param <Type>        input/output type of the transformation
     * @return a compiled function
     */
    public <Type> Function2 <Type, Type, Type> compile(ReduceDescriptor<Type> descriptor) {
        return new Function2<Type, Type, Type>()  {
            @Override
            public Type call(Type i0, Type i1) throws Exception {
                return descriptor.getJavaImplementation().apply(i0, i1);
            }
        };
    }
}
