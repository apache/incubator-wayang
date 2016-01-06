package org.qcri.rheem.core.plan;

/**
 * An output slot declares an output of an {@link Operator}.
 */
public class OutputSlot extends Slot {

    public OutputSlot(OutputSlot blueprint, Operator owner) {
        this(blueprint.getName(), owner);
    }

    public OutputSlot(String name, Operator owner) {
        super(name, owner);
    }

    public OutputSlot copyFor(Operator owner) {
        return new OutputSlot(this, owner);
    }
}
