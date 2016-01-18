package org.qcri.rheem.core.plan;

/**
 * Helper class for the implementation of the {@link Operator} interface.
 */
public abstract class OperatorBase implements Operator {

    private CompositeOperator parent;

    private int epoch = FIRST_EPOCH;

    protected final InputSlot<?>[] inputSlots;

    protected final OutputSlot<?>[] outputSlots;

    public OperatorBase(InputSlot<?>[] inputSlots, OutputSlot<?>[] outputSlots, CompositeOperator parent) {
        this.parent = parent;
        this.inputSlots = inputSlots;
        this.outputSlots = outputSlots;
    }

    public OperatorBase(int numInputSlots, int numOutputSlots, CompositeOperator parent) {
        this(new InputSlot[numInputSlots], new OutputSlot[numOutputSlots], parent);
    }

    @Override
    public InputSlot<?>[] getAllInputs() {
        return this.inputSlots;
    }

    @Override
    public OutputSlot<?>[] getAllOutputs() {
        return this.outputSlots;
    }

    @Override
    public CompositeOperator getParent() {
        return this.parent;
    }

    @Override
    public void setParent(CompositeOperator parent) {
        this.parent = parent;
    }

    @Override
    public int getEpoch() {
        return epoch;
    }

    @Override
    public void setEpoch(int epoch) {
        this.epoch = epoch;
    }

    /**
     * Convenience method to set the epoch.
     */
    public Operator at(int epoch) {
        this.setEpoch(epoch);
        return this;
    }

    @Override
    public String toString() {
        return String.format("%s[%d->%d, id=%x]",
                this.getClass().getSimpleName(),
                this.getNumInputs(),
                this.getNumOutputs(),
//                this.getParent() == null ? "top-level" : "nested",
                this.hashCode());
    }
}
