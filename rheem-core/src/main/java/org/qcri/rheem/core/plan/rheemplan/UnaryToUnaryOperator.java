package org.qcri.rheem.core.plan.rheemplan;

import org.qcri.rheem.core.types.DataSetType;

/**
 * This operator has a single input and a single output.
 */
public abstract class UnaryToUnaryOperator<InputType, OutputType> extends OperatorBase implements ElementaryOperator {

    /**
     * Creates a new instance.
     */
    protected UnaryToUnaryOperator(DataSetType<InputType> inputType,
                                   DataSetType<OutputType> outputType,
                                   boolean isSupportingBroadcastInputs) {
        super(1, 1, isSupportingBroadcastInputs);
        this.inputSlots[0] = new InputSlot<>("in", this, inputType);
        this.outputSlots[0] = new OutputSlot<>("out", this, outputType);
    }

    /**
     * Copies the given instance.
     *
     * @see UnaryToUnaryOperator#UnaryToUnaryOperator(DataSetType, DataSetType, boolean)
     * @see OperatorBase#OperatorBase(OperatorBase)
     */
    protected UnaryToUnaryOperator(UnaryToUnaryOperator<InputType, OutputType> that) {
        super(that);
        this.inputSlots[0] = new InputSlot<>("in", this, that.getInputType());
        this.outputSlots[0] = new OutputSlot<>("out", this, that.getOutputType());
    }

    @SuppressWarnings("unchecked")
    public InputSlot<InputType> getInput() {
        return (InputSlot<InputType>) this.getInput(0);
    }

    @SuppressWarnings("unchecked")
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
