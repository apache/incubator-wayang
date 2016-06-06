package org.qcri.rheem.spark.compiler;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * Wraps a {@link Function} as a {@link FlatMapFunction}.
 */
public class MapFunctionToMapPartitionFunctionAdapter<InputType, OutputType> implements FlatMapFunction<Iterator<InputType>, OutputType> {

    private final Function<InputType, OutputType> function;

    public MapFunctionToMapPartitionFunctionAdapter(Function<InputType, OutputType> function) {
        this.function = function;
    }

    @Override
    public Iterable<OutputType> call(Iterator<InputType> it) throws Exception {
        List<OutputType> out = new ArrayList<>();
        while (it.hasNext()) {
            out.add(function.apply(it.next()));
        }
        return out;
    }
}