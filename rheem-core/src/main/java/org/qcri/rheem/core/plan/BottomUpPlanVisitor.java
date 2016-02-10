package org.qcri.rheem.core.plan;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * Visitor (as in the Visitor Pattern) for {@link PhysicalPlan}s.
 */
public abstract class BottomUpPlanVisitor<Payload, Return> {

//    public Map<InputSlot, Return> process(Operator operator, InputSlot<?> fromInputSlot, Payload payload) {
//        final Optional<Return> returnOptional = prepareVisit(operator, fromInputSlot, payload);
//        if (returnOptional != null) {
//            return returnOptional.orElse(null);
//        }
//        Map<InputSlot, Return> result = operator.accept(this, fromInputSlot, payload);
//        followUp(operator, fromInputSlot, payload, result);
//
//        return result;
//    }
//
//    protected abstract Optional<Return> prepareVisit(Operator operator, InputSlot<?> fromInputSlot, Payload payload);
//
//    protected abstract void followUp(Operator operator, InputSlot<?> fromInputSlot, Payload payload, Map<InputSlot, Return> result);
//
//    /**
//     * todo
//     *
//     * @param operatorAlternative
//     */
//    public abstract Return visit(OperatorAlternative operatorAlternative, InputSlot<?> fromInputSlot, Payload payload);
//
//    public Map<InputSlot, Return> visit(Subplan subplan, InputSlot<?> fromInputSlot, Payload payload) {
//        if (fromInputSlot == null) {
//            return subplan.getSource().accept(this, fromInputSlot, payload);
//        } else {
//            final Collection<InputSlot<Object>> inputSlots = subplan.followInput(fromInputSlot.unchecked());
//            for (InputSlot<Object> inputSlot : inputSlots) {
//                inputSlot.getOwner().accept(this, inputSlot, payload);
//            }
//        }
//    }
//
//    /**
//     * todo
//     */
//    public abstract Return visit(ActualOperator operator, InputSlot<?> fromInputSlot, Payload payload);
//
//    protected Optional<Return> proceed(Operator operator, int inputIndex, Payload payload) {
//        final InputSlot<Object> outerInputSlot = operator
//                .getOutermostInputSlot(operator.getInput(inputIndex))
//                .unchecked();
//        final OutputSlot<Object> occupant = outerInputSlot.getOccupant();
//        if (occupant != null) {
//            return Optional.ofNullable(process(occupant.getOwner(), occupant, payload));
//        } else {
//            return null;
//        }
//    }

}
