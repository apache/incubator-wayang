package org.qcri.rheem.core.plan.rheemplan;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.types.DataSetType;
import org.slf4j.LoggerFactory;

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
    private final DataSetType<T> type;

    /**
     * Can assign a {@link CardinalityEstimate} to this instance, which is then associated to a certain {@link RheemPlan}.
     */
    private CardinalityEstimate cardinalityEstimate;

    protected Slot(String name, Operator owner, DataSetType<T> type) {
        this.name = name;
        this.owner = owner;
        this.type = type;
    }

    public String getName() {
        return this.name;
    }

    public Operator getOwner() {
        return this.owner;
    }

    public DataSetType<T> getType() {
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
        return String.format("%s[%s of %s]",
                this.getClass().getSimpleName(),
//                this.type,
                this.name,
                this.owner == null ? "no owner" : this.owner.toString());
    }

    /**
     * @return the index of this slot within its owner
     * @throws IllegalStateException if this slot does not have an owner
     * @see #getOwner()
     */
    public abstract int getIndex() throws IllegalStateException;

    /**
     * Get the currently assigned {@link CardinalityEstimate}.
     *
     * @return the {@link CardinalityEstimate} or {@code null} if none
     */
    public CardinalityEstimate getCardinalityEstimate() {
        return this.cardinalityEstimate;
    }

    /**
     * Assign a {@link CardinalityEstimate} to this instance.
     *
     * @param cardinalityEstimate the {@link CardinalityEstimate} to assign
     */
    public void setCardinalityEstimate(CardinalityEstimate cardinalityEstimate) {
        if (this.cardinalityEstimate != null) {
            LoggerFactory.getLogger(this.getClass()).warn("Reassigning cardinality.");
        }
        this.cardinalityEstimate = cardinalityEstimate;
    }
}
