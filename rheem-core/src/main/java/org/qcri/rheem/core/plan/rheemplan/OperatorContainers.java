package org.qcri.rheem.core.plan.rheemplan;

import org.apache.commons.lang3.Validate;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Utilities to deal with {@link OperatorContainer}s.
 */
public class OperatorContainers {

    /**
     * Check whether the combination of {@link InputSlot}s and {@link OutputSlot}s describe more than one {@link Operator}.
     *
     * @param inputs  said {@link InputSlot}s
     * @param outputs said {@link OutputSlot}s
     * @return whether it is worth wrapping
     */
    public static boolean canWrap(List<InputSlot<?>> inputs, List<OutputSlot<?>> outputs) {
        // Play safe: we only propose not to wrap if we cover all Slots of an Operator.
        Operator operator = null;
        for (InputSlot<?> input : inputs) {
            if (operator == null) operator = input.getOwner();
            else if (operator != input.getOwner()) return false;
        }
        for (OutputSlot<?> output : outputs) {
            if (operator == null) operator = output.getOwner();
            else if (operator != output.getOwner()) return false;
        }
        return inputs.size() == operator.getNumInputs() && outputs.size() == operator.getNumOutputs();
    }

    /**
     * Wraps an {@link Operator} in an {@link OperatorContainer}.
     *
     * @see #wrap(List, List, OperatorContainer)
     */
    public static void wrap(Operator wrappee, OperatorContainer wrapper) {
        wrap(Arrays.asList(wrappee.getAllInputs()), Arrays.asList(wrappee.getAllOutputs()), wrapper);
    }

    /**
     * Wraps the {@link Operator}s between the given {@link InputSlot}s and {@link OutputSlot}s in the given
     * {@link OperatorContainer}.
     * <p>In detail, the corresponding {@link CompositeOperator}'s {@link Slot}s are created and intercept
     * the given {@link Slot}s occupants/"occupees". The wrapped {@link Operator}s are assigned the given
     * {@link OperatorContainer} as container.</p>
     *
     * @param inputs    mentioned {@link InputSlot}s, aligned with the {@link CompositeOperator}'s {@link InputSlot}s
     * @param outputs   mentioned {@link OutputSlot}s, aligned with the {@link CompositeOperator}'s {@link OutputSlot}s
     * @param container that should wrap the subplan
     */
    public static void wrap(List<InputSlot<?>> inputs, List<OutputSlot<?>> outputs, OperatorContainer container) {
        final SlotMapping slotMapping = container.getSlotMapping();
        final Operator containerOperator = container.toOperator();

        // Copy and steal the inputSlots.
        for (int inputIndex = 0; inputIndex < inputs.size(); inputIndex++) {
            InputSlot<?> innerInput = inputs.get(inputIndex);
            final InputSlot<?> outerInput = containerOperator.getInput(inputIndex);
            outerInput.unchecked().stealOccupant(innerInput.unchecked());
            slotMapping.mapUpstream(innerInput, outerInput);
        }

        // Copy and steal the outputSlots.
        for (int outputIndex = 0; outputIndex < outputs.size(); outputIndex++) {
            OutputSlot<?> innerOutput = outputs.get(outputIndex);
            final OutputSlot<?> outerOutput = containerOperator.getOutput(outputIndex);
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

    /**
     * Moves the contained {@link Operator}s from a {@code source} to a {@code target} {@link OperatorContainer}.
     *
     * @param source {@link OperatorContainer} whose {@link CompositeOperator} has identical {@link Slot}s as that of the {@code target}
     * @param target {@link OperatorContainer} whose {@link CompositeOperator} has identical {@link Slot}s as that of the {@code source}
     */
    public static void move(OperatorContainer source, OperatorContainer target) {
        final CompositeOperator sourceOperator = source.toOperator();
        final CompositeOperator targetOperator = target.toOperator();
        Operators.assertEqualInputs(sourceOperator, targetOperator);
        Operators.assertEqualOutputs(sourceOperator, targetOperator);

        for (int inputIndex = 0; inputIndex < sourceOperator.getNumInputs(); inputIndex++) {
            InputSlot<Object> sourceInput = sourceOperator.getInput(inputIndex).unchecked();
            InputSlot<Object> targetInput = targetOperator.getInput(inputIndex).unchecked();
            for (InputSlot<Object> innerInput : source.followInput(sourceInput)) {
                target.getSlotMapping().mapUpstream(innerInput, targetInput);
            }
        }
        for (int outputIndex = 0; outputIndex < sourceOperator.getNumOutputs(); outputIndex++) {
            OutputSlot<Object> sourceOutput = sourceOperator.getOutput(outputIndex).unchecked();
            OutputSlot<Object> targetOutput = targetOperator.getOutput(outputIndex).unchecked();
            OutputSlot<Object> innerOutput = source.traceOutput(sourceOutput);
            target.getSlotMapping().mapUpstream(targetOutput, innerOutput);
        }

        source.getContainedOperators().forEach(o -> o.setContainer(target));
    }
}
