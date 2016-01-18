package org.qcri.rheem.core.plan;

/**
 * Operator free of ambiguities.
 */
public interface ActualOperator extends Operator {

    @Override
    default <Payload, Return> Return accept(PlanVisitor<Payload, Return> visitor, OutputSlot<?> outputSlot, Payload payload) {
        return visitor.visit(this, outputSlot, payload);
    }
}
