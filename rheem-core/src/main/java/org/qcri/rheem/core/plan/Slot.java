package org.qcri.rheem.core.plan;

/**
 * Abstract class for inputs and outputs to operators.
 *
 * @param <T> type of elements that can be passed through this slot
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
     * Class of {@link T}.
     */
    private final Class<T> type;

    protected Slot(String name, Operator owner, Class<T> type) {
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

    public Class<T> getType() {
        return this.type;
    }
}
