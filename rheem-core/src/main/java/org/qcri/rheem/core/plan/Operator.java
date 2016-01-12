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

    InputSlot<?>[] getAllInputs();

    OutputSlot<?>[] getAllOutputs();

    default InputSlot<?> getInput(int index) {
        final InputSlot[] allInputs = getAllInputs();
        if (index < 0 || index >= allInputs.length) {
            throw new IllegalArgumentException(String.format("Illegal input index: %d.", index));
        }
        return allInputs[index];
    }

    default OutputSlot<?> getOutput(int index) {
        final OutputSlot[] allOutputs = getAllOutputs();
        if (index < 0 || index >= allOutputs.length) {
            throw new IllegalArgumentException(String.format("Illegal output index: %d.", index));
        }
        return allOutputs[index];
    }

    default InputSlot<?> getInput(String name) {
        for (InputSlot inputSlot : getAllInputs()) {
            if (inputSlot.getName().equals(name)) return inputSlot;
        }
        throw new IllegalArgumentException(String.format("No slot with such name: %s", name));
    }

    default OutputSlot<?> getOutput(String name) {
        for (OutputSlot outputSlot : getAllOutputs()) {
            if (outputSlot.getName().equals(name)) return outputSlot;
        }
        throw new IllegalArgumentException(String.format("No slot with such name: %s", name));
    }

    /**
     * Connect an output of this operator to the input of a second operator.
     *
     * @param thisOutputIndex index of the output slot to connect to
     * @param that            operator to connect to
     * @param thatInputIndex  index of the input slot to connect from
     */
    default <T> void connectTo(int thisOutputIndex, Operator that, int thatInputIndex) {
        final InputSlot<T> inputSlot = (InputSlot<T>) that.getInput(thatInputIndex);
        final OutputSlot<T> outputSlot = (OutputSlot<T>) this.getOutput(thisOutputIndex);
        if (!inputSlot.getType().isCompatibleTo(outputSlot.getType())) {
            throw new IllegalArgumentException("Cannot connect slots: mismatching types");
        }
        outputSlot.connectTo(inputSlot);
    }


    /**
     * Retrieve the operator that is connected to the input at the given index.
     *
     * @param inputIndex the index of the input to consider
     * @return the input operator or {@code null} if no such operator exists
     */
    default Operator getInputOperator(int inputIndex) {
        final OutputSlot occupant = getInput(inputIndex).getOccupant();
        return occupant == null ? null : occupant.getOwner();
    }

    /**
     * Tells whether this operator is a sink, i.e., it has no outputs.
     *
     * @return whether this operator is a sink
     */
    default boolean isSink() {
        return this.getNumOutputs() == 0;
    }

    /**
     * Tells whether this operator is a source, i.e., it has no inputs.
     *
     * @return whether this operator is a source
     */
    default boolean isSource() {
        return this.getNumInputs() == 0;
    }

    /**
     * todo
     */
    void accept(PlanVisitor visitor);

    /**
     * Operators can be nested in other operators, e.g., in a {@link Subplan} or a {@link OperatorAlternative}.
     *
     * @return the parent of this operator or {@code null} if this is a top-level operator
     */
    Operator getParent();

    /**
     * Operators can be nested in other operators, e.g., in a {@link Subplan} or a {@link OperatorAlternative}.
     *
     * @param newParent the new parent of this operator or {@code null} to declare it top-level
     */
    void setParent(Operator newParent);

}
