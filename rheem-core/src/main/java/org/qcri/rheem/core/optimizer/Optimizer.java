package org.qcri.rheem.core.optimizer;

import org.qcri.rheem.core.plan.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Dummy implementation: just resolve alternatives by looking for those alternatives that are execution operators.
 */
public class Optimizer {

    public PhysicalPlan buildExecutionPlan(PhysicalPlan rheemPlan) {
        PhysicalPlan executionPlan = new PhysicalPlan();

//        Optimizer.Pass pass = new Optimizer.Pass();
//        for (Operator sink : rheemPlan.getSinks()) {
//            Operator executionSink = pass.pick(sink, null, null);
//            executionPlan.addSink(executionSink);
//        }
        Optimizer.Pass2 pass2 = new Optimizer.Pass2();
        for (Operator sink : rheemPlan.getSinks()) {
            Operator executionSink = pass2.process(sink, null, null);
            executionPlan.addSink(executionSink);
        }

        return executionPlan;
    }

    private static class Pass {

        Map<OperatorAlternative, OperatorAlternative.Alternative> pickedAlternatives = new HashMap<>();

        Map<Operator, ExecutionOperator> executionOperators = new HashMap<>();

        Map<OutputSlot<?>, OutputSlot<?>> slotTranslation = new HashMap<>();

        private Operator pick(Operator operator,
                              OutputSlot<Object> fromOutputSlot,
                              InputSlot<Object> pendingExecOpInputSlot) {

            // Simple case: we have already created the requested output slot.
            if (fromOutputSlot != null && slotTranslation.containsKey(fromOutputSlot)) {
                final OutputSlot<Object> execOpOutputSlot = this.slotTranslation.get(fromOutputSlot).unchecked();
                execOpOutputSlot.connectTo(pendingExecOpInputSlot);
                return execOpOutputSlot.getOwner();
            }

            // Otherwise, we need to do some translation.
            if (operator instanceof Subplan) {
                // If the operator is a subplan, delegate the picking.
                if (fromOutputSlot == null) {
                    return pick(((Subplan) operator).enter(), fromOutputSlot, pendingExecOpInputSlot);
                } else {
                    final OutputSlot<Object> innerOutputSlot = ((Subplan) operator).enter(fromOutputSlot).unchecked();
                    return pick(innerOutputSlot.getOwner(), innerOutputSlot, pendingExecOpInputSlot);
                }

            } else if (operator instanceof OperatorAlternative) {
                // If the operator is an alternative, check at first if we already settled upon an alternative.
                final OperatorAlternative operatorAlternative = (OperatorAlternative) operator;
                OperatorAlternative.Alternative pickedAlternative;
                ExecutionOperator executionOperator;

                if (this.pickedAlternatives.containsKey(operatorAlternative)) {
                    pickedAlternative = this.pickedAlternatives.get(operatorAlternative);
                    executionOperator = this.executionOperators.get(operator);

                } else {
                    pickedAlternative = operatorAlternative.getAlternatives().stream()
                            .filter(alternative -> alternative.getOperator() instanceof ExecutionOperator)
                            .findFirst()
                            .orElseThrow(() -> new RuntimeException("No executable alternative found."));
                    this.pickedAlternatives.put(operatorAlternative, pickedAlternative);
                    executionOperator = ((ExecutionOperator) pickedAlternative.getOperator()).copy();
                    this.executionOperators.put(operator, executionOperator);

                    // We have a new operator -> we need to translate its children as well.
                    for (int inputIndex = 0; inputIndex < operator.getNumInputs(); inputIndex++) {
                        final InputSlot<Object> outerInputSlot = operator
                                .getOutermostInputSlot(operator.getInput(inputIndex))
                                .unchecked();
                        final OutputSlot<Object> occupant = outerInputSlot.getOccupant();
                        if (occupant != null) {
                            pick(occupant.getOwner(), occupant, executionOperator.getInput(inputIndex).unchecked());
                        }
                    }
                }

                if (Objects.nonNull(fromOutputSlot)) {
                    final OutputSlot<Object> innerOutputSlot = pickedAlternative.getSlotMapping().resolve(fromOutputSlot);
                    OutputSlot<Object> execOpOutputSlot = executionOperator.getOutput(innerOutputSlot.getIndex()).unchecked();
                    execOpOutputSlot.connectTo(pendingExecOpInputSlot);
                }

                return executionOperator;

            } else {
                throw new RuntimeException("Unforeseen operator type: " + operator);
            }
        }
    }

    private static class Pass2 extends PlanVisitor<InputSlot<Object>, Operator> {

        Map<OperatorAlternative, OperatorAlternative.Alternative> pickedAlternatives = new HashMap<>();

        Map<Operator, ExecutionOperator> executionOperators = new HashMap<>();

        Map<OutputSlot<?>, OutputSlot<?>> slotTranslation = new HashMap<>();

        @Override
        protected Optional<Operator> prepareVisit(Operator operator, OutputSlot<?> fromOutputSlot, InputSlot<Object> pendingExecOpInputSlot) {
            // Simple case: we have already created the requested output slot.
            if (fromOutputSlot != null && slotTranslation.containsKey(fromOutputSlot)) {
                final OutputSlot<Object> execOpOutputSlot = this.slotTranslation.get(fromOutputSlot).unchecked();
                execOpOutputSlot.connectTo(pendingExecOpInputSlot);
                return Optional.of(execOpOutputSlot.getOwner());
            }
            return null;
        }

        @Override
        protected void followUp(Operator operator, OutputSlot<?> fromOutputSlot, InputSlot<Object> inputSlot, Operator result) {

        }

        @Override
        public Operator visit(OperatorAlternative operatorAlternative, OutputSlot<?> fromOutputSlot, InputSlot<Object> pendingExecOpInputSlot) {
            // If the operator is an alternative, check at first if we already settled upon an alternative.
            OperatorAlternative.Alternative pickedAlternative;
            ExecutionOperator executionOperator;

            if (this.pickedAlternatives.containsKey(operatorAlternative)) {
                pickedAlternative = this.pickedAlternatives.get(operatorAlternative);
                executionOperator = this.executionOperators.get(operatorAlternative);

            } else {
                pickedAlternative = operatorAlternative.getAlternatives().stream()
                        .filter(alternative -> alternative.getOperator() instanceof ExecutionOperator)
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException("No executable alternative found."));
                this.pickedAlternatives.put(operatorAlternative, pickedAlternative);
                executionOperator = ((ExecutionOperator) pickedAlternative.getOperator()).copy();
                this.executionOperators.put(operatorAlternative, executionOperator);

                // We have a new operator -> we need to translate its children as well.
                for (int inputIndex = 0; inputIndex < operatorAlternative.getNumInputs(); inputIndex++) {
                    proceed(operatorAlternative, inputIndex, executionOperator.getInput(inputIndex).unchecked());
                }

            }

            if (Objects.nonNull(fromOutputSlot)) {
                final OutputSlot<Object> innerOutputSlot = pickedAlternative.getSlotMapping().resolve(fromOutputSlot.unchecked());
                OutputSlot<Object> execOpOutputSlot = executionOperator.getOutput(innerOutputSlot.getIndex()).unchecked();
                execOpOutputSlot.connectTo(pendingExecOpInputSlot);
            }

            return executionOperator;
        }

        @Override
        public Operator visit(ActualOperator operator, OutputSlot<?> fromOutputSlot, InputSlot<Object> inputSlot) {
            throw new RuntimeException("Unforeseen operator type: " + operator);
        }
    }

}
