package org.qcri.rheem.core.plan;

/**
 * Abstract class for inputs and outputs to operators.
 */
abstract public class Slot {

    /**
     * Identifies this slot within its operator.
     */
    private final String name;

    /**
     * The operator that is being decorated by this slot.
     */
    private final Operator owner;

    protected Slot(String name, Operator owner) {
        this.name = name;
        this.owner = owner;
    }

    public String getName() {
        return name;
    }

    public Operator getOwner() {
        return owner;
    }
}
