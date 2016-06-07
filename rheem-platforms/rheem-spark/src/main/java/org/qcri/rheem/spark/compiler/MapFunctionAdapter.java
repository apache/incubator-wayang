package org.qcri.rheem.spark.compiler;

import org.apache.spark.api.java.function.Function;

/**
 * Wraps a {@link java.util.function.Function} as a {@link Function}.
 */
public class MapFunctionAdapter<InputType, OutputType> implements Function<InputType, OutputType> {

    private java.util.function.Function<InputType, OutputType> function;

    public MapFunctionAdapter(java.util.function.Function<InputType, OutputType> function) {
        this.function = function;
    }

    @Override
    public OutputType call(InputType dataQuantum) throws Exception {
        return this.function.apply(dataQuantum);
    }
}
