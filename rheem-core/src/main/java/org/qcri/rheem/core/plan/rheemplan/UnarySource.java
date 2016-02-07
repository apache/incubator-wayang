package org.qcri.rheem.core.plan.rheemplan;

import org.qcri.rheem.core.types.DataSetType;

/**
 * Abstract base class for sources with a single output.
 */
public abstract class UnarySource<T> extends OperatorBase implements ActualOperator {

    public UnarySource(DataSetType type, OperatorContainer container) {
        super(0, 1, container);
        this.outputSlots[0] = new OutputSlot<T>("output", this, type);
    }

    public OutputSlot<T> getOutput() {
        return (OutputSlot<T>) getOutput(0);
    }

    public DataSetType<T> getType() {
        return getOutput().getType();
    }

}
