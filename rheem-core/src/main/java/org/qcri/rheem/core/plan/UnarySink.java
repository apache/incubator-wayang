package org.qcri.rheem.core.plan;

import org.qcri.rheem.core.types.DataSet;

/**
 * Abstract base-class for sinks with a single input.
 */
public abstract class UnarySink<T> extends OperatorBase implements ActualOperator {

    public UnarySink(DataSet type, Operator parent) {
        super(1, 0, parent);
        this.inputSlots[0] = new InputSlot<>("input", this, type);
    }

    public InputSlot<T> getInput() {
        return (InputSlot<T>) getInput(0);
    }

}
