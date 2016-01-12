package org.qcri.rheem.core.plan;

/**
 * Operator free of ambiguities.
 */
public interface ActualOperator extends Operator {

    @Override
    default void accept(PlanVisitor visitor) {
        visitor.visit(this);
    }
}
