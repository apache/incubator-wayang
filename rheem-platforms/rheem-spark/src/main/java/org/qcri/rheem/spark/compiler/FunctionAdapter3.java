package org.qcri.rheem.spark.compiler;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Wraps a {@link Function} as a {@link FlatMapFunction}.
 */
public class FunctionAdapter3<InputType, OutputType> implements FlatMapFunction<Iterator<InputType>, OutputType> {

    private Function<InputType, OutputType> function;

    public FunctionAdapter3 (Function function) {
        this.function = function;
    }

    @Override
    public Iterable<OutputType> call(Iterator<InputType> it) throws Exception {
        List<OutputType> out = new ArrayList<>();
        it.forEachRemaining(o -> {
            try {
                out.add(function.call(o));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        return out;
    }
}