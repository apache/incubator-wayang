package org.qcri.rheem.spark.compiler;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Iterator;

/**
 * Wraps a {@link java.util.function.Function} as a {@link FlatMapFunction}.
 */
public class FlatMapFunctionAdapter<InputType, OutputType> implements FlatMapFunction<InputType, OutputType> {

    private java.util.function.Function<InputType, Iterable<OutputType>> function;

    public FlatMapFunctionAdapter(java.util.function.Function<InputType, Iterable<OutputType>> function) {
        this.function = function;
    }

    @Override
    public Iterator<OutputType> call(InputType dataQuantum) throws Exception {
        return this.function.apply(dataQuantum).iterator();
    }
}
