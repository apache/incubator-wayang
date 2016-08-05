package org.qcri.rheem.core.plan.rheemplan;

import java.util.Collection;

/**
 * A composite operator can be decomposed into smaller operators.
 */
public interface CompositeOperator extends Operator {

    @Override
    default boolean isElementary() {
        return false;
    }

    /**
     * Acknowledge that the given old operator has been replaced with a new one.
     *
     * @param oldOperator the operator that has been replaced
     * @param newOperator the new operator
     */
    void noteReplaced(Operator oldOperator, Operator newOperator);

    /**
     * Get the {@link OperatorContainer}s of this instance.
     *
     * @return the {@link OperatorContainer}s
     */
    Collection<OperatorContainer> getContainers();

}
