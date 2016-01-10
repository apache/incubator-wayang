package org.qcri.rheem.basic.operators;

import org.qcri.rheem.basic.function.MapFunctionDescriptor;
import org.qcri.rheem.core.plan.OneToOneOperator;

/**
 * A map operator represents semantics as they are known from frameworks, such as Spark and Flink. It pulls each
 * available element from the input slot, applies a function to it, and pushes that element to the output slot.
 */
public class MapOperator<InputType, OutputType> extends OneToOneOperator<InputType, OutputType> {

    /**
     * Function that this operator applies to the input elements.
     */
    private final MapFunctionDescriptor<InputType, OutputType> functionDescriptor;

    /**
     * Creates a new instance.
     *
     * @param inputTypeClass  class of the input types (i.e., type of {@link #getInput()}
     * @param outputTypeClass class of the output types (i.e., type of {@link #getOutput()}
     */
    public MapOperator(Class<InputType> inputTypeClass, Class<OutputType> outputTypeClass,
                       MapFunctionDescriptor<InputType, OutputType> functionDescriptor) {
        super(inputTypeClass, outputTypeClass);
        this.functionDescriptor = functionDescriptor;
    }

    public MapFunctionDescriptor<InputType, OutputType> getFunctionDescriptor() {
        return functionDescriptor;
    }
}
