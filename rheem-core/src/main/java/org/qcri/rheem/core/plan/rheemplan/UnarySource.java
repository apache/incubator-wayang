package org.qcri.rheem.core.plan.rheemplan;

import org.qcri.rheem.core.types.DataSetType;

/**
 * Abstract base class for sources with a single output.
 */
public abstract class UnarySource<T> extends OperatorBase implements ElementaryOperator {

    /**
     * Creates a new instance.
     */
    public UnarySource(DataSetType type, boolean isSupportingBroadcastInputs, OperatorContainer container) {
        super(0, 1, isSupportingBroadcastInputs, container);
        this.outputSlots[0] = new OutputSlot<T>("output", this, type);
    }

    /**
     * Creates a new instance that does not support broadcast {@link InputSlot}s.
     */
    public UnarySource(DataSetType type, OperatorContainer container) {
        this(type, false, container);
    }

    public OutputSlot<T> getOutput() {
        return (OutputSlot<T>) this.getOutput(0);
    }

    public DataSetType<T> getType() {
        return this.getOutput().getType();
    }

}
