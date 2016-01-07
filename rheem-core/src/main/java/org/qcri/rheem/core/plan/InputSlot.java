package org.qcri.rheem.core.plan;

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

    public InputSlot(InputSlot blueprint, Operator owner) {
        this(blueprint.getName(), owner, blueprint.getType());
    }

    public InputSlot(String name, Operator owner, Class<T> type) {
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

    public OutputSlot getOccupant() {
        return occupant;
    }
}
