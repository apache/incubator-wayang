package org.qcri.rheem.core.optimizer;

import org.qcri.rheem.core.plan.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Dummy implementation: just resolve alternatives by looking for those alternatives that are execution operators.
 */
public class DummyOptimizer {

    public PhysicalPlan buildExecutionPlan(PhysicalPlan rheemPlan) {
        PhysicalPlan executionPlan = new PhysicalPlan();

        Pass pass = new Pass();
        for (Operator sink : rheemPlan.getSinks()) {
            Operator executionSink = pass.process(sink, null, null);
            executionPlan.addSink(executionSink);
        }

        return executionPlan;
    }

    private static class Pass extends TopDownPlanVisitor<InputSlot<Object>, Operator> {

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
                final OutputSlot<Object> innerOutputSlot = pickedAlternative.getSlotMapping().resolveUpstream(fromOutputSlot.unchecked());
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
