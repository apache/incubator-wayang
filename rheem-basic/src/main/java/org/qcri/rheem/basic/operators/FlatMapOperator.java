package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.plan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Iterator;


/**
 * A flatmap operator represents semantics as they are known from frameworks, such as Spark and Flink. It pulls each
 * available element from the input slot, applies a function to it, returning zero or more output elements,
 * flattening the result and pushes it to the output slot.
 */
public class FlatMapOperator<InputType, OutputType> extends UnaryToUnaryOperator<InputType, OutputType> {

    /**
     * Function that this operator applies to the input elements.
     */
    protected final FlatMapDescriptor<InputType, Iterator<OutputType>> functionDescriptor;

    /**
     * Creates a new instance.
     */
    public FlatMapOperator(DataSetType<InputType> inputType, DataSetType<OutputType> outputType,
                           FlatMapDescriptor<InputType, Iterator<OutputType>> functionDescriptor) {
        super(inputType, outputType, null);
        this.functionDescriptor = functionDescriptor;
    }

    public FlatMapDescriptor<InputType, Iterator<OutputType>> getFunctionDescriptor() {
        return functionDescriptor;
    }
}
