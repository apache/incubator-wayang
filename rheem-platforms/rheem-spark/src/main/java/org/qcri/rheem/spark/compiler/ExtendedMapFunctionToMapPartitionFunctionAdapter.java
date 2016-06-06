package org.qcri.rheem.spark.compiler;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.spark.execution.SparkExecutionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * Wraps a {@link Function} as a {@link FlatMapFunction}.
 */
public class ExtendedMapFunctionToMapPartitionFunctionAdapter<InputType, OutputType> implements FlatMapFunction<Iterator<InputType>, OutputType> {

    private final FunctionDescriptor.ExtendedSerializableFunction<InputType, OutputType> impl;

    private final SparkExecutionContext executionContext;

    public ExtendedMapFunctionToMapPartitionFunctionAdapter(FunctionDescriptor.ExtendedSerializableFunction<InputType, OutputType> extendedFunction,
                                                            SparkExecutionContext sparkExecutionContext) {
        this.impl = extendedFunction;
        this.executionContext = sparkExecutionContext;
    }

    @Override
    public Iterable<OutputType> call(Iterator<InputType> it) throws Exception {
        this.impl.open(executionContext);
        List<OutputType> out = new ArrayList<>();
        while (it.hasNext()) {
            out.add(this.impl.apply(it.next()));
        }
        return out;
    }
}