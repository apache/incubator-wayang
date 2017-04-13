package org.qcri.rheem.spark.compiler;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.util.Iterators;
import org.qcri.rheem.spark.execution.SparkExecutionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * Wraps a {@link Function} as a {@link FlatMapFunction}.
 */
public class ExtendedMapPartitionsFunctionAdapter<InputType, OutputType>
        implements FlatMapFunction<Iterator<InputType>, OutputType> {

    private final FunctionDescriptor.ExtendedSerializableFunction<Iterable<InputType>, Iterable<OutputType>> impl;

    private final SparkExecutionContext executionContext;

    public ExtendedMapPartitionsFunctionAdapter(
            FunctionDescriptor.ExtendedSerializableFunction<Iterable<InputType>, Iterable<OutputType>> extendedFunction,
            SparkExecutionContext sparkExecutionContext) {
        this.impl = extendedFunction;
        this.executionContext = sparkExecutionContext;
    }

    @Override
    public Iterator<OutputType> call(Iterator<InputType> it) throws Exception {
        this.impl.open(executionContext);
        List<OutputType> out = new ArrayList<>();
        while (it.hasNext()) {
            final Iterable<OutputType> mappedPartition = this.impl.apply(Iterators.wrapWithIterable(it));
            for (OutputType dataQuantum : mappedPartition) {
                out.add(dataQuantum);
            }
        }
        return out.iterator();
    }
}