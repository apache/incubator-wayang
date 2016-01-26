package org.qcri.rheem.core.plan;

import org.qcri.rheem.core.types.DataSetType;

/**
 * This operator has a single input and a single output.
 */
public abstract class BinaryToUnaryOperator<InputType1, InputType2, OutputType> extends OperatorBase implements ActualOperator {

    /**
     * Creates a new instance.
     */
    public BinaryToUnaryOperator(DataSetType<InputType1> inputType1,
                                 DataSetType<InputType2> inputType2,
                                 DataSetType<OutputType> outputType) {
        super(2, 1, null);
        this.inputSlots[0] = new InputSlot<>("input1", this, inputType1);
        this.inputSlots[1] = new InputSlot<>("input2", this, inputType2);
        this.outputSlots[0] = new OutputSlot<>("output", this, outputType);
    }

    public InputSlot<InputType1> getInput1() {
        return (InputSlot<InputType1>) getInput(0);
    }

    public InputSlot<InputType2> getInput2() {
        return (InputSlot<InputType2>) getInput(1);
    }

    public OutputSlot<OutputType> getOutput() {
        return (OutputSlot<OutputType>) getOutput(0);
    }

}
