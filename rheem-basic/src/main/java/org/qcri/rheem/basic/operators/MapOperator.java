package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

/**
 * A map operator represents semantics as they are known from frameworks, such as Spark and Flink. It pulls each
 * available element from the input slot, applies a function to it, and pushes that element to the output slot.
 */
public class MapOperator<InputType, OutputType> extends UnaryToUnaryOperator<InputType, OutputType> {

    /**
     * Function that this operator applies to the input elements.
     */
    protected final TransformationDescriptor<InputType, OutputType> functionDescriptor;

    /**
     * Creates a new instance.
     */
    public MapOperator(DataSetType<InputType> inputType, DataSetType<OutputType> outputType,
                       TransformationDescriptor<InputType, OutputType> functionDescriptor) {
        super(inputType, outputType, null);
        this.functionDescriptor = functionDescriptor;
    }

    public TransformationDescriptor<InputType, OutputType> getFunctionDescriptor() {
        return functionDescriptor;
    }
}
