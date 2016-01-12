package org.qcri.rheem.core.plan;

import org.qcri.rheem.core.types.DataSet;

/**
 * Abstract base class for sources with a single output.
 */
public abstract class UnarySource<T> extends OperatorBase implements ActualOperator {

    public UnarySource(DataSet type, Operator parent) {
        super(0, 1, parent);
        this.outputSlots[0] = new OutputSlot<T>("output", this, type);
    }

    public OutputSlot<T> getOutput() {
        return (OutputSlot<T>) getOutput(0);
    }

}
