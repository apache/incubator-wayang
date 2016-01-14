package org.qcri.rheem.core.plan;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * This mapping can be used to encapsulate subplans by connecting slots (usually <b>against</b> the data flow direction,
 * i.e., outer output slot -> inner output slot, inner input slot -> outer input slot).
 */
public class SlotMapping {

    private final Map<Slot, Slot> mapping = new HashMap<>();

    public static SlotMapping wrap(Operator wrappee, Operator wrapper) {
        SlotMapping slotMapping = new SlotMapping();
        slotMapping.mapAll(wrapper.getAllOutputs(), wrappee.getAllOutputs());
        slotMapping.mapAll(wrappee.getAllInputs(), wrapper.getAllInputs());
        return slotMapping;
    }

    public void mapAll(InputSlot[] sources, InputSlot[] targets) {
        if (sources.length != targets.length) throw new IllegalArgumentException();
        for (int i = 0; i < sources.length; i++) {
            map(sources[i], targets[i]);
        }
    }

    public void mapAll(OutputSlot[] sources, OutputSlot[] targets) {
        if (sources.length != targets.length) throw new IllegalArgumentException();
        for (int i = 0; i < sources.length; i++) {
            map(sources[i], targets[i]);
        }
    }

    public void map(InputSlot<?> source, InputSlot<?> target) {
        if (!source.isCompatibleWith(target)) {
            throw new IllegalArgumentException(String.format("Incompatible slots given: %s -> %s", source, target));
        }

        this.mapping.put(source, target);
    }

    public void map(OutputSlot<?> source, OutputSlot<?> target) {
        if (!source.isCompatibleWith(target)) {
            throw new IllegalArgumentException(String.format("Incompatible slots given: %s -> %s", source, target));
        }

        this.mapping.put(source, target);
    }

    public <T> InputSlot<T> resolve(InputSlot<T> source) {
        return (InputSlot<T>) this.mapping.get(source);
    }

    public <T> OutputSlot<T> resolve(OutputSlot<T> source) {
        return (OutputSlot<T>) this.mapping.get(source);
    }

    /**
     * Replace the mappings from an old, wrapped operator with a new wrapped operator.
     * @param oldOperator the old wrapped operator
     * @param newOperator the new wrapped operator
     */
    public void replaceInputSlotMappings(Operator oldOperator, Operator newOperator) {
        if (oldOperator.getNumInputs() != newOperator.getNumInputs()) {
            throw new IllegalArgumentException("Operators are not matching.");
        }

        for (int i = 0; i < oldOperator.getNumInputs(); i++) {
            final InputSlot<?> oldInput = oldOperator.getInput(i);
            final InputSlot<?> newInput = newOperator.getInput(i);

            final InputSlot<?> outerInput = resolve(oldInput);
            if (outerInput != null) {
                map(newInput, outerInput);
            }
        }
    }

    /**
     * Replace the mappings from an old, wrapped operator with a new wrapped operator.
     * @param oldOperator the old wrapped operator
     * @param newOperator the new wrapped operator
     */
    public void replaceOutputSlotMappings(Operator oldOperator, Operator newOperator) {
        if (oldOperator.getNumOutputs() != newOperator.getNumOutputs()) {
            throw new IllegalArgumentException("Operators are not matching.");
        }

        for (int i = 0; i < oldOperator.getNumOutputs(); i++) {
            final OutputSlot<?> oldOutput = oldOperator.getOutput(i);
            final OutputSlot<?> newOutput = newOperator.getOutput(i);

            this.mapping.entrySet().stream()
                    .filter(entry -> entry.getValue() == oldOutput)
                    .findFirst()
                    .map(Map.Entry::getKey)
                    .ifPresent(outerOutput -> this.map((OutputSlot<?>) outerOutput, newOutput));
        }
    }
}
