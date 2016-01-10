package org.qcri.rheem.core.plan;

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
     *
     * @param inputTypeClass  class of the input types (i.e., type of {@link #getInput()}
     * @param outputTypeClass class of the output types (i.e., type of {@link #getOutput()}
     */
    public OneToOneOperator(Class<InputType> inputTypeClass, Class<OutputType> outputTypeClass) {
        this.inputSlot = new InputSlot<>("input", this, inputTypeClass);
        this.inputSlots = new InputSlot[]{this.inputSlot};
        this.outputSlot = new OutputSlot<>("output", this, outputTypeClass);
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

    public Class<InputType> getInputType() {
        return this.inputSlot.getType();
    }

    public Class<OutputType> getOutputType() {
        return this.outputSlot.getType();
    }
}
