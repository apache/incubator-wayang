package org.qcri.rheem.core.plan;

import org.qcri.rheem.core.types.DataSet;

/**
 * This operator has a single input and a single output.
 */
public abstract class OneToOneOperator<InputType, OutputType> implements Operator {

    private final InputSlot<InputType> inputSlot;

    private final OutputSlot<OutputType> outputSlot;

    private final InputSlot[] inputSlots;

    private final OutputSlot[] outputSlots;

    /**
     * Creates a new instance.
     */
    public OneToOneOperator(DataSet inputType, DataSet outputType) {
        this.inputSlot = new InputSlot<>("input", this, inputType);
        this.inputSlots = new InputSlot[]{this.inputSlot};
        this.outputSlot = new OutputSlot<>("output", this, outputType);
        this.outputSlots = new OutputSlot[]{this.outputSlot};
    }

    @Override
    public InputSlot[] getAllInputs() {
        return this.inputSlots;
    }

    @Override
    public OutputSlot[] getAllOutputs() {
        return this.outputSlots;
    }

    public InputSlot<InputType> getInput() {
        return this.inputSlot;
    }

    public OutputSlot<OutputType> getOutput() {
        return this.outputSlot;
    }

    public DataSet getInputType() {
        return this.inputSlot.getType();
    }

    public DataSet getOutputType() {
        return this.outputSlot.getType();
    }
}
