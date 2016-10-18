package org.qcri.rheem.core.plan.rheemplan;

import org.qcri.rheem.core.types.DataSetType;

import java.util.List;

/**
 * An input slot declares an input of an {@link Operator}.
 *
 * @param <T> see {@link Slot}
 */
public class InputSlot<T> extends Slot<T> {

    /**
     * Output slot of another operator that is connected to this input slot.
     */
    private OutputSlot<T> occupant;

    /**
     * Tells whether this instance represents a broadcasted input.
     */
    private final boolean isBroadcast;

    /**
     * Copy the {@link InputSlot}s of a given {@link Operator}.
     */
    public static void mock(Operator template, Operator mock) {
        mock(template, mock, true);
    }

    /**
     * Copy the {@link InputSlot}s of a given {@link Operator}.
     */
    public static void mock(Operator template, Operator mock, boolean isKeepBroadcastStatus) {
        if (template.getNumInputs() != mock.getNumInputs()) {
            throw new IllegalArgumentException("Cannot mock inputs: Mismatching number of inputs.");
        }

        InputSlot[] mockSlots = mock.getAllInputs();
        for (int i = 0; i < template.getNumInputs(); i++) {
            mockSlots[i] = isKeepBroadcastStatus ?
                    template.getInput(i).copyFor(mock) :
                    template.getInput(i).copyAsNonBroadcastFor(mock);
        }
    }

    /**
     * Copy the {@link InputSlot}s to a given {@link Operator}.
     */
    public static void mock(List<InputSlot<?>> inputSlots, Operator mock, boolean isKeepBroadcastStatus) {
        if (inputSlots.size() != mock.getNumInputs()) {
            throw new IllegalArgumentException("Cannot mock inputs: Mismatching number of inputs.");
        }

        InputSlot[] mockSlots = mock.getAllInputs();
        int i = 0;
        for (InputSlot<?> inputSlot : inputSlots) {
            mockSlots[i++] = isKeepBroadcastStatus ?
                    inputSlot.copyFor(mock) :
                    inputSlot.copyAsNonBroadcastFor(mock);
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
            thief.getInput(i).unchecked().stealOccupant(victim.getInput(i).unchecked());
        }
    }

    /**
     * Takes away the occupant {@link OutputSlot} of the {@code victim} and connects it to this instance.
     */
    public void stealOccupant(InputSlot<T> victim) {
        if (victim.getOccupant() == null) return;
        assert this.getOccupant() == null : String.format(
                "%s cannot steal %s's occuppant %s, because there already is %s.",
                this, victim, victim.getOccupant(), this.getOccupant()
        );
        final OutputSlot<T> occupant = victim.getOccupant();
        if (occupant != null) {
            occupant.disconnectFrom(victim);
            occupant.connectTo(this);
        }
    }

    /**
     * Creates a new instance that imitates the given {@code blueprint}, but for a different {@code owner}.
     */
    public InputSlot(Slot<T> blueprint, Operator owner) {
        this(blueprint.getName(), owner, blueprint.getType());
    }

    /**
     * Creates a new instance that imitates the given {@code blueprint}, but for a different {@code owner}.
     */
    public InputSlot(InputSlot<T> blueprint, Operator owner) {
        this(blueprint.getName(), owner, blueprint.isBroadcast(), blueprint.getType());
    }

    /**
     * Creates a new, non-broadcast instance.
     */
    public InputSlot(String name, Operator owner, DataSetType<T> type) {
        this(name, owner, false, type);
    }

    /**
     * Creates a new instance.
     */
    public InputSlot(String name, Operator owner, boolean isBroadcast, DataSetType<T> type) {
        super(name, owner, type);
        this.isBroadcast = isBroadcast;
    }

    /**
     * Shortcut for {@link #InputSlot(Slot, Operator)}
     */
    public InputSlot<T> copyFor(Operator owner) {
        return new InputSlot<>(this, owner);
    }

    /**
     * As {@link #copyFor(Operator)}, but ensures that the copy will not be marked as broadcast.
     */
    public InputSlot<T> copyAsNonBroadcastFor(Operator owner) {
        return new InputSlot<>(this.getName(), owner, false, this.getType());
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
    InputSlot<T> setOccupant(OutputSlot<T> outputSlot) {
        this.occupant = outputSlot;
        return this;
    }

    public OutputSlot<T> getOccupant() {
        return this.occupant;
    }

    @Override
    public int getIndex() throws IllegalStateException {
        if (this.index != -1) return this.index;

        assert this.getOwner() != null : "This slot has no owner.";
        for (int i = 0; i < this.getOwner().getNumInputs(); i++) {
            if (this.getOwner().getInput(i) == this) {
                return this.index = i;
            }
        }
        throw new IllegalStateException("Could not find this slot within its owner.");
    }

    @SuppressWarnings("unchecked")
    public InputSlot<Object> unchecked() {
        return (InputSlot<Object>) this;
    }

    /**
     * @return whether this is a broadcast
     */
    public boolean isBroadcast() {
        return this.isBroadcast;
    }

    /**
     * @return whether this instance is designated to close feedback loops (i.e., data flow cycles)
     */
    public boolean isFeedback() {
        return this.getOwner().isFeedbackInput(this);
    }

    /**
     * Notifies this instance that it has been detached from its {@link #occupant}.
     */
    public void notifyDetached() {
        if (this.isBroadcast) {
            // TODO: Consider removing broadacast.
        }
    }

    /**
     * Tells whether this instance is inside of a {@link LoopSubplan} and consumes an {@link OutputSlot} outside
     * of that {@link LoopSubplan}.
     *
     * @return whether above condition is satisfied
     */
    public boolean isLoopInvariant() {
        final Operator owner = this.getOwner();

        // If this is a LoopHeadOperator initial/iteration input, we know that this is not loop invariant.
        if (owner.isLoopHead() && (
                ((LoopHeadOperator) owner).getLoopBodyInputs().contains(this)
                        || ((LoopHeadOperator) owner).getLoopInitializationInputs().contains(this)
        )) return false;

        // Find the loop this instance is in.
        final LoopSubplan innermostLoop = owner.getInnermostLoop();
        if (innermostLoop == null) return false;

        // Find the adjacent OutputSlot.
        final InputSlot<T> outerInput = owner.getOutermostInputSlot(this);
        final OutputSlot<T> occupant = outerInput.getOccupant();
        if (occupant == null) return false;

        // Check if the adjacent OutputSlot is in a different loop.
        return occupant.getOwner().getInnermostLoop() != innermostLoop;
    }
}
