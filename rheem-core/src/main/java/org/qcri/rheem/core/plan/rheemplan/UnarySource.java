package org.qcri.rheem.core.plan.rheemplan;

import org.qcri.rheem.core.types.DataSetType;

/**
 * Abstract base class for sources with a single output.
 */
public abstract class UnarySource<T> extends OperatorBase implements ElementaryOperator {

    /**
     * Creates a new instance that does not support broadcast {@link InputSlot}s.
     */
    public UnarySource(DataSetType<T> type) {
        this(type, false);
    }

    /**
     * Creates a new instance.
     */
    public UnarySource(DataSetType<T> type, boolean isSupportingBroadcastInputs) {
        super(0, 1, isSupportingBroadcastInputs);
        this.outputSlots[0] = new OutputSlot<>("out", this, type);
    }

    /**
     * Copies the given instance.
     *
     * @see UnarySource#UnarySource(DataSetType, boolean)
     * @see OperatorBase#OperatorBase(OperatorBase)
     */
    protected UnarySource(UnarySource<T> that) {
        super(that);
        this.outputSlots[0] = new OutputSlot<>("output", this, that.getType());
    }

    @SuppressWarnings("unchecked")
    public OutputSlot<T> getOutput() {
        return (OutputSlot<T>) this.getOutput(0);
    }

    public DataSetType<T> getType() {
        return this.getOutput().getType();
    }

}
