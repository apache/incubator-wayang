package org.qcri.rheem.core.plan.rheemplan;

import org.qcri.rheem.core.types.DataSetType;

/**
 * Abstract base-class for sinks with a single input.
 */
public abstract class UnarySink<T> extends OperatorBase implements ElementaryOperator {

    /**
     * Creates a new instance that does not support broadcast {@link InputSlot}s.
     */
    public UnarySink(DataSetType<T> type) {
        this(type, false);
    }

    /**
     * Creates a new instance.
     */
    public UnarySink(DataSetType<T> type, boolean isSupportingBroadcastInputs) {
        super(1, 0, isSupportingBroadcastInputs);
        this.inputSlots[0] = new InputSlot<>("in", this, type);
    }

    /**
     * Copies the given instance.
     *
     * @see UnarySink#UnarySink(DataSetType, boolean)
     * @see OperatorBase#OperatorBase(OperatorBase)
     */
    public UnarySink(UnarySink<T> that) {
        super(that);
        this.inputSlots[0] = new InputSlot<>("in", this, that.getType());
    }

    @SuppressWarnings("unchecked")
    public InputSlot<T> getInput() {
        return (InputSlot<T>) this.getInput(0);
    }

    public DataSetType<T> getType() {
        return this.getInput().getType();
    }

}
