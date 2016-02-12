package org.qcri.rheem.core.plan.rheemplan;

import org.qcri.rheem.core.types.DataSetType;

import java.util.Objects;

/**
 * An input slot declares an input of an {@link Operator}.
 *
 * @param <T> see {@link Slot}
 */
public class InputSlot<T> extends Slot<T> {

    /**
     * Output slot of another operator that is connected to this input slot.
     */
    private OutputSlot occupant;

    /**
     * Copy the {@link InputSlot}s of a given {@link Operator}.
     */
    public static void mock(Operator template, Operator mock) {
        if (template.getNumInputs() != mock.getNumInputs()) {
            throw new IllegalArgumentException("Cannot mock inputs: Mismatching number of inputs.");
        }

        InputSlot[] mockSlots = mock.getAllInputs();
        for (int i = 0; i < template.getNumInputs(); i++) {
            mockSlots[i] = template.getInput(i).copyFor(mock);
        }
    }

    /**
     * Take the input connections away from one operator and give them to another one.
     */
    public static void stealConnections(Operator victim, Operator thief) {
        if (victim.getNumInputs() != thief.getNumInputs()) {
            throw new IllegalArgumentException("Cannot steal inputs: Mismatching number of inputs.");
        }

        for (int i = 0; i < victim.getNumInputs(); i++) {
            final OutputSlot<?> occupant = victim.getInput(i).getOccupant();
            if (occupant != null) {
                occupant.unchecked().disconnectFrom(victim.getInput(i).unchecked());
                occupant.unchecked().connectTo(thief.getInput(i).unchecked());
            }
        }
    }

    public InputSlot(InputSlot blueprint, Operator owner) {
        this(blueprint.getName(), owner, blueprint.getType());
    }

    public InputSlot(String name, Operator owner, DataSetType type) {
        super(name, owner, type);
    }

    public InputSlot copyFor(Operator owner) {
        return new InputSlot(this, owner);
    }

    /**
     * Connects the given {@link OutputSlot}. Consider using the interface of the {@link OutputSlot} instead to
     * keep consistency of connections in the plan.
     *
     * @param outputSlot the output slot to connect to
     * @return this instance
     * @see OutputSlot#connectTo(InputSlot)
     * @see OutputSlot#disconnectFrom(InputSlot)
     */
    InputSlot setOccupant(OutputSlot outputSlot) {
        this.occupant = outputSlot;
        return this;
    }

    public OutputSlot<T> getOccupant() {
        return this.occupant;
    }

    @Override
    public int getIndex() throws IllegalStateException {
        if (Objects.isNull(this.getOwner())) throw new IllegalStateException("This slot has no owner.");
        for (int i = 0; i < this.getOwner().getNumInputs(); i++) {
            if (this.getOwner().getInput(i) == this) return i;
        }
        throw new IllegalStateException("Could not find this slot within its owner.");
    }

    @SuppressWarnings("unchecked")
    public InputSlot<Object> unchecked() {
        return (InputSlot<Object>) this;
    }

    /**
     * Recursively trace the given {@code inputSlot}.
     *
     * @param inputSlot the {@link InputSlot} to trace
     * @return the {@link InputSlot} of the outermost {@link Operator} that represents the given {@code inputSlot} or
     * {@code null} if the {@code inputSlot} has no representation in the outermost {@link Operator}.
     * @see Operator#getContainer()
     * @see OperatorContainer#traceInput(InputSlot)
     */
    public static <T> InputSlot<T> traceOutermostInput(InputSlot<T> inputSlot) {
        while (inputSlot != null && inputSlot.getOwner().getContainer() != null) {
            final OperatorContainer container = inputSlot.getOwner().getContainer();
            inputSlot = container.traceInput(inputSlot);
        }

        return inputSlot;
    }
}
