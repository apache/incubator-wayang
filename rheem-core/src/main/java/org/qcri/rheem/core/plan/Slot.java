package org.qcri.rheem.core.plan;

import org.qcri.rheem.core.types.DataSetType;

/**
 * Abstract class for inputs and outputs to operators.
 */
abstract public class Slot<T> {

    /**
     * Identifies this slot within its operator.
     */
    private final String name;

    /**
     * The operator that is being decorated by this slot.
     */
    private final Operator owner;

    /**
     * Type of data passed through this slot, expressed as a {@link DataSetType} so as to define not only the types of
     * elements that are passed but also capture their structure (e.g., flat, grouped, sorted, ...).
     */
    private final DataSetType type;

    protected Slot(String name, Operator owner, DataSetType type) {
        this.name = name;
        this.owner = owner;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public Operator getOwner() {
        return owner;
    }

    public DataSetType getType() {
        return this.type;
    }

    /**
     * @return whether this is an {@link OutputSlot}
     */
    public boolean isOutputSlot() {
        return this instanceof OutputSlot;
    }

    /**
     * @return whether this is an input slot
     */
    public boolean isInputSlot() {
        return this instanceof InputSlot;
    }

    public boolean isCompatibleWith(Slot<?> that) {
        return this.type.equals(that.type);
    }

    @Override
    public String toString() {
        return String.format("%s[%s, %s]",
                getClass().getSimpleName(),
                this.type,
                this.owner == null ? "no owner" : this.owner.getClass().getSimpleName());
    }
}
