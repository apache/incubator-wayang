package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.stream.Stream;

/**
 * A flatmap operator represents semantics as they are known from frameworks, such as Spark and Flink. It pulls each
 * available element from the input slot, applies a function to it, returning zero or more output elements,
 * flattening the result and pushes it to the output slot.
 */
public class FlatMapOperator<InputType, OutputType> extends UnaryToUnaryOperator<InputType, OutputType> {

    /**
     * Function that this operator applies to the input elements.
     */
    protected final TransformationDescriptor<InputType, Stream<OutputType>> functionDescriptor;

    /**
     * Creates a new instance.
     */
    public FlatMapOperator(DataSetType<InputType> inputType, DataSetType<OutputType> outputType,
                       TransformationDescriptor<InputType, Stream<OutputType>> functionDescriptor) {
        super(inputType, outputType, null);
        this.functionDescriptor = functionDescriptor;
    }

    public TransformationDescriptor<InputType, Stream<OutputType>> getFunctionDescriptor() {
        return functionDescriptor;
    }
}
