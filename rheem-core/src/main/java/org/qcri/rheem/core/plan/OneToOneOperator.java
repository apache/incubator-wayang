package org.qcri.rheem.core.plan;

/**
 * This operator has a single input and a single output.
 */
public abstract class OneToOneOperator implements Operator {

    private final InputSlot[] inputSlots = new InputSlot[1];

    private final OutputSlot[] outputSlots = new OutputSlot[1];

    @Override
    public InputSlot[] getAllInputs() {
        return this.inputSlots;
    }

    @Override
    public OutputSlot[] getAllOutputs() {
        return this.outputSlots;
    }

    public InputSlot getInput() {
        return getInput(0);
    }

    public OutputSlot getOutput() {
        return getOutput(0);
    }
}
