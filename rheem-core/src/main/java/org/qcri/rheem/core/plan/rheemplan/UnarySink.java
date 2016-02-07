package org.qcri.rheem.core.plan.rheemplan;

import org.qcri.rheem.core.types.DataSetType;

/**
 * Abstract base-class for sinks with a single input.
 */
public abstract class UnarySink<T> extends OperatorBase implements ActualOperator {

    public UnarySink(DataSetType type, OperatorContainer container) {
        super(1, 0, container);
        this.inputSlots[0] = new InputSlot<>("input", this, type);
    }

    public InputSlot<T> getInput() {
        return (InputSlot<T>) getInput(0);
    }

    public DataSetType<T> getType() {
        return getInput().getType();
    }

}
