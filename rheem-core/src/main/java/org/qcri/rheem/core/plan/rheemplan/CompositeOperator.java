package org.qcri.rheem.core.plan.rheemplan;

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

}
