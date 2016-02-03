package org.qcri.rheem.core.plan;

import org.qcri.rheem.core.types.DataSetType;

/**
 * This operator has 2 inputs and a single output.
 */
public abstract class BinaryToUnaryOperator<InputType0, InputType1, OutputType> extends OperatorBase
        implements ActualOperator {

    /**
     * Creates a new instance.
     */
    public BinaryToUnaryOperator(DataSetType<InputType0> inputType0, DataSetType<InputType1> inputType1,
                                 DataSetType<OutputType> outputType,
                                 CompositeOperator parent) {
        super(2, 1, parent);
        this.inputSlots[0] = new InputSlot<>("input0", this, inputType0);
        this.inputSlots[1] = new InputSlot<>("input1", this, inputType1);
        this.outputSlots[0] = new OutputSlot<>("output", this, outputType);
    }


    public DataSetType<InputType0> getInputType0() {
        return ((InputSlot<InputType0>) getInput(0)).getType();
    }

    public DataSetType<InputType1> getInputType1() {
        return ((InputSlot<InputType1>) getInput(1)).getType();
    }


    public DataSetType<OutputType> getOutputType() {
        return ((OutputSlot<OutputType>) getOutput(0)).getType();
    }
}
