package org.qcri.rheem.core.plan.rheemplan;

import org.apache.commons.lang3.Validate;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Utilities to deal with {@link OperatorContainer}s.
 */
public class OperatorContainers {

    /**
     * TODO
     * @param inputs aligned with container#getOp#getInputs...
     * @param outputs
     * @param container
     */
    public static void wrap(
            List<InputSlot<?>> inputs,
            List<OutputSlot<?>> outputs,
            OperatorContainer container) {

        final SlotMapping slotMapping = container.getSlotMapping();
        final Operator containerOperator = container.toOperator();

        // Copy and steal the inputSlots.
        for (int inputIndex = 0; inputIndex < inputs.size(); inputIndex++) {
            InputSlot<?> innerInput = inputs.get(inputIndex);
            final InputSlot<?> outerInput = innerInput.copyFor(containerOperator);
            containerOperator.setInput(inputIndex, outerInput);
            outerInput.unchecked().stealOccupant(innerInput.unchecked());
            slotMapping.mapUpstream(innerInput, outerInput);
        }

        // Copy and steal the outputSlots.
        for (int outputIndex = 0; outputIndex < outputs.size(); outputIndex++) {
            OutputSlot<?> innerOutput = outputs.get(outputIndex);
            final OutputSlot<?> outerOutput = innerOutput.copyFor(containerOperator);
            containerOperator.setOutput(outputIndex, outerOutput);
            outerOutput.unchecked().stealOccupiedSlots(innerOutput.unchecked());
            slotMapping.mapUpstream(outerOutput, innerOutput);
        }


        // Mark all contained Operators and detect sources and sinks.
        final Set<InputSlot<?>> inputSet = new HashSet<>(inputs);
        final Set<OutputSlot<?>> outputSet = new HashSet<>(outputs);
        PlanTraversal.fanOut()
                .followingInputsIf(input -> !inputSet.contains(input))
                .followingOutputsIf(output -> !outputSet.contains(output))
                .withCallback(operator -> {
                    operator.setContainer(container);
                    if (operator.isSink()) {
                        Validate.isTrue(containerOperator.isSink(), "Detected sink %s in non-sink %s.", operator, containerOperator);
                        Validate.isTrue(container.getSink() == null, "At least two sinks %s and %s in %s.", operator, container.getSink(), containerOperator);
                        container.setSink(operator);
                    }
                    if (operator.isSource()) {
                        Validate.isTrue(containerOperator.isSource(), "Detected source %s in non-source %s.", operator, containerOperator);
                        Validate.isTrue(container.getSource() == null, "At least two sources %s and %s in %s.", operator, container.getSource(), containerOperator);
                        container.setSource(operator);
                    }
                })
                .traverse(Stream.concat(inputSet.stream(), outputSet.stream()).map(Slot::getOwner));

        // Sanity checks.
        Validate.isTrue((container.getSource() == null) ^ containerOperator.isSource());
        Validate.isTrue((container.getSink() == null) ^ containerOperator.isSink());
    }
}
