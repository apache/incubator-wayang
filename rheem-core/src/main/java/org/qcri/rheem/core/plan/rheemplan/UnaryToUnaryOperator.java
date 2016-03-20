package org.qcri.rheem.core.plan.rheemplan;

import org.qcri.rheem.core.types.DataSetType;

/**
 * This operator has a single input and a single output.
 */
public abstract class UnaryToUnaryOperator<InputType, OutputType> extends OperatorBase implements ElementaryOperator {

    /**
     * Creates a new instance.
     */
    public UnaryToUnaryOperator(DataSetType inputType, DataSetType outputType, boolean isSupportingBroadcastInputs,
                                OperatorContainer container) {
        super(1, 1, isSupportingBroadcastInputs, container);
        this.inputSlots[0] = new InputSlot<>("input", this, inputType);
        this.outputSlots[0] = new OutputSlot<>("output", this, outputType);
    }

    public InputSlot<InputType> getInput() {
        return (InputSlot<InputType>) this.getInput(0);
    }

    public OutputSlot<OutputType> getOutput() {
        return (OutputSlot<OutputType>) this.getOutput(0);
    }

    public DataSetType<InputType> getInputType() {
        return this.getInput().getType();
    }

    public DataSetType<OutputType> getOutputType() {
        return this.getOutput().getType();
    }
}
