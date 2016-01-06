package org.qcri.rheem.core.plan;

/**
 * An input slot declares an input of an {@link Operator}.
 */
public class InputSlot extends Slot {


    /**
     * Output slot of another operator that is connected to this input slot.
     */
    private OutputSlot occupant;

    public InputSlot(InputSlot blueprint, Operator owner) {
        this(blueprint.getName(), owner);
    }

    public InputSlot(String name, Operator owner) {
        super(name, owner);
    }

    public InputSlot copyFor(Operator owner) {
        return new InputSlot(this, owner);
    }

    /**
     * Connects the given {@link OutputSlot}.
     *
     * @param outputSlot the output slot to connect to
     * @return this instance
     */
    public InputSlot setOccupant(OutputSlot outputSlot) {
        this.occupant = outputSlot;
        return this;
    }

    public OutputSlot getOccupant() {
        return occupant;
    }
}
