package org.qcri.rheem.basic.operators;

import org.qcri.rheem.basic.function.MapFunctionDescriptor;
import org.qcri.rheem.core.plan.OneToOneOperator;
import org.qcri.rheem.core.types.DataSet;

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
     */
    public MapOperator(DataSet inputType, DataSet outputType,
                       MapFunctionDescriptor<InputType, OutputType> functionDescriptor) {
        super(inputType, outputType);
        this.functionDescriptor = functionDescriptor;
    }

    public MapFunctionDescriptor<InputType, OutputType> getFunctionDescriptor() {
        return functionDescriptor;
    }
}
