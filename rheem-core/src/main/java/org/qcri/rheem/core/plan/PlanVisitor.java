package org.qcri.rheem.core.plan;

import java.util.Optional;

/**
 * Visitor (as in the Visitor Pattern) for {@link PhysicalPlan}s.
 */
public abstract class PlanVisitor<Payload, Return> {

    public Return process(Operator operator, OutputSlot<?> fromOutputSlot, Payload payload) {
        final Optional<Return> returnOptional = prepareVisit(operator, fromOutputSlot, payload);
        if (returnOptional != null) {
            return returnOptional.orElse(null);
        }
        Return result = operator.accept(this, fromOutputSlot, payload);
        followUp(operator, fromOutputSlot, payload, result);

        return result;
    }

    protected abstract Optional<Return> prepareVisit(Operator operator, OutputSlot<?> fromOutputSlot, Payload payload);

    protected abstract void followUp(Operator operator, OutputSlot<?> fromOutputSlot, Payload payload, Return result);

    /**
     * todo
     *
     * @param operatorAlternative
     */
    public abstract Return visit(OperatorAlternative operatorAlternative, OutputSlot<?> fromOutputSlot, Payload payload);

    public Return visit(Subplan subplan, OutputSlot<?> fromOutputSlot, Payload payload) {
        if (fromOutputSlot == null) {
            return subplan.enter().accept(this, fromOutputSlot, payload);
        } else {
            final OutputSlot<Object> innerOutputSlot = subplan.enter(fromOutputSlot).unchecked();
            return innerOutputSlot.getOwner().accept(this, innerOutputSlot, payload);
        }
    }

    /**
     * todo
     */
    public abstract Return visit(ActualOperator operator, OutputSlot<?> fromOutputSlot, Payload payload);

    protected Optional<Return> proceed(Operator operator, int inputIndex, Payload payload) {
        final InputSlot<Object> outerInputSlot = operator
                .getOutermostInputSlot(operator.getInput(inputIndex))
                .unchecked();
        final OutputSlot<Object> occupant = outerInputSlot.getOccupant();
        if (occupant != null) {
            return Optional.ofNullable(process(occupant.getOwner(), occupant, payload));
        } else {
            return null;
        }
    }

}
