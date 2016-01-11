package org.qcri.rheem.core.plan;

import org.qcri.rheem.core.types.DataSet;

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
     * Type of data passed through this slot, expressed as a {@link DataSet} so as to define not only the types of
     * elements that are passed but also capture their structure (e.g., flat, grouped, sorted, ...).
     */
    private final DataSet type;

    protected Slot(String name, Operator owner, DataSet type) {
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

    public DataSet getType() {
        return this.type;
    }
}
