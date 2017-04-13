package org.qcri.rheem.spark.compiler;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.qcri.rheem.core.util.Iterators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * Wraps a {@link Function} as a {@link FlatMapFunction}.
 */
public class MapPartitionsFunctionAdapter<InputType, OutputType> implements FlatMapFunction<Iterator<InputType>, OutputType> {

    private final Function<Iterable<InputType>, Iterable<OutputType>> function;

    public MapPartitionsFunctionAdapter(Function<Iterable<InputType>, Iterable<OutputType>> function) {
        this.function = function;
    }

    @Override
    public Iterator<OutputType> call(Iterator<InputType> it) throws Exception {
        List<OutputType> out = new ArrayList<>();
        while (it.hasNext()) {
            final Iterable<OutputType> mappedPartition = this.function.apply(Iterators.wrapWithIterable(it));
            for (OutputType dataQuantum : mappedPartition) {
                out.add(dataQuantum);
            }
        }
        return out.iterator();
    }
}