package org.qcri.rheem.core.plan;

/**
 * A composite operator can be decomposed into smaller operators.
 */
public interface CompositeOperator extends Operator {

    @Override
    default boolean isElementary() {
        return false;
    }

    /**
     * Find the {@link SlotMapping} that connects the {@code child} with the outside world.
     *
     * @param child a child of this operator
     * @return the {@link SlotMapping} that wraps the {@code child} or {@code null} if there is no such mapping
     */
    SlotMapping getSlotMappingFor(Operator child);

    /**
     * Acknowledge that the given old operator has been replaced with a new one.
     * @param oldOperator the operator that has been replaced
     * @param newOperator the new operator
     */
    void replace(Operator oldOperator, Operator newOperator);
}
