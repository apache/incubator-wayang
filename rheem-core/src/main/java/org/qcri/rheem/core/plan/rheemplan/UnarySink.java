package org.qcri.rheem.core.plan.rheemplan;

import org.qcri.rheem.core.types.DataSetType;

/**
 * Abstract base-class for sinks with a single input.
 */
public abstract class UnarySink<T> extends OperatorBase implements ActualOperator {

    /**
     * Creates a new instance.
     */
    public UnarySink(DataSetType type, boolean isSupportingBroadcastInputs, OperatorContainer container) {
        super(1, 0, isSupportingBroadcastInputs, container);
        this.inputSlots[0] = new InputSlot<>("input", this, type);
    }

    /**
     * Creates a new instance that does not support broadcast {@link InputSlot}s.
     */
    public UnarySink(DataSetType type, OperatorContainer container) {
        this(type, false, container);
    }

    public InputSlot<T> getInput() {
        return (InputSlot<T>) this.getInput(0);
    }

    public DataSetType<T> getType() {
        return this.getInput().getType();
    }

}
