package org.qcri.rheem.core.plan;

/**
 * A subplan encapsulates connected operators as a single operator.
 */
public class Subplan extends OperatorBase implements ActualOperator {

    /**
     * Maps input and output slots <b>against</b> the direction of the data flow.
     */
    private final SlotMapping slotMapping;

    private final Operator inputOperator, outputOperator;

    public Subplan(Operator inputOperator, Operator outputOperator, Operator parent) {
        super(inputOperator.getNumInputs(), outputOperator.getNumOutputs(), parent);

        this.inputOperator = inputOperator;
        this.outputOperator = outputOperator;
        this.slotMapping = new SlotMapping();

        // Copy the interface of the input operator, steal its connections, and map the slots.
        InputSlot.mock(inputOperator, this);
        InputSlot.stealConnections(inputOperator, this);
        this.slotMapping.mapAll(inputOperator.getAllInputs(), this.inputSlots);

        // Copy the interface of the output operator, steal its connections, and map the slots.
        OutputSlot.mock(outputOperator, this);
        OutputSlot.stealConnections(outputOperator, this);
        this.slotMapping.mapAll(this.outputSlots, inputOperator.getAllOutputs());

        // Traverse through the subplan and become the parent of the operators.
        new PlanTraversal(true, false)
                .withCallback(operator -> operator.setParent(this))
                .traverse(this.outputOperator);
    }

    public SlotMapping getSlotMapping() {
        return slotMapping;
    }

    /**
     * Enter this subplan. This subplan needs to be a sink.
     *
     * @return the sink operator within this subplan
     */
    public Operator enter() {
        if (!isSink()) {
            throw new IllegalArgumentException("Cannot enter subplan: no output slot given and subplan is not a sink.");
        }

        return this.outputOperator;
    }

    /**
     * Enter this subplan by following one of its output slots.
     *
     * @param subplanOutputSlot an output slot of this subplan
     * @return the output within the subplan that is connected to the given output slot
     */
    public <T> OutputSlot<T> enter(OutputSlot<T> subplanOutputSlot) {
        // If this subplan is not a sink, we trace the given output slot via the slot mapping.
        if (subplanOutputSlot.getOwner() != this) {
            throw new IllegalArgumentException("Cannot enter subplan: Output slot does not belong to this subplan.");
        }

        final OutputSlot<T> resolvedSlot = this.slotMapping.resolve(subplanOutputSlot);
        if (resolvedSlot != null && resolvedSlot.getOwner().getParent() != this) {
            throw new IllegalStateException("Traced to an output slot whose owner is not a child of this subplan.");
        }
        return resolvedSlot;
    }

    public <T> InputSlot<T> exit(InputSlot<T> innerInputSlot) {
        if (innerInputSlot.getOwner().getParent() != this) {
            throw new IllegalArgumentException("Trying to exit from an input slot that is not within this subplan.");
        }
        return this.slotMapping.resolve(innerInputSlot);
    }

    public Subplan exit(Operator innerOperator) {
        if (!isSource()) {
            throw new IllegalArgumentException("Cannot exit subplan: no input slot given and subplan is not a source.");
        }

        return innerOperator == this.inputOperator ? this : null;
    }

    // TODO: develop constructors/factory methods to deal with more than one input and output operator

}
