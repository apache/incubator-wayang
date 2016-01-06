package org.qcri.rheem.core.plan;

/**
 * An operator is any node that within a data flow plan.
 */
public interface Operator {

    default int getNumInputs() {
        return getAllInputs().length;
    }

    default int getNumOutputs() {
        return getAllOutputs().length;
    }

    InputSlot[] getAllInputs();

    OutputSlot[] getAllOutputs();

    default InputSlot getInput(int index) {
        final InputSlot[] allInputs = getAllInputs();
        if (index < 0 || index >= allInputs.length) {
            throw new IllegalArgumentException(String.format("Illegal input index: %d.", index));
        }
        return allInputs[index];
    }

    default OutputSlot getOutput(int index) {
        final OutputSlot[] allOutputs = getAllOutputs();
        if (index < 0 || index >= allOutputs.length) {
            throw new IllegalArgumentException(String.format("Illegal output index: %d.", index));
        }
        return allOutputs[index];
    }

    default InputSlot getInput(String name) {
        for (InputSlot inputSlot : getAllInputs()) {
            if (inputSlot.getName().equals(name)) return inputSlot;
        }
        throw new IllegalArgumentException(String.format("No slot with such name: %s", name));
    }

    default OutputSlot getOutput(String name) {
        for (OutputSlot outputSlot : getAllOutputs()) {
            if (outputSlot.getName().equals(name)) return outputSlot;
        }
        throw new IllegalArgumentException(String.format("No slot with such name: %s", name));
    }

}
