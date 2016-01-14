package org.qcri.rheem.core.plan;

/**
 * A subplan encapsulates connected operators as a single operator.
 */
public class Subplan extends OperatorBase implements ActualOperator, CompositeOperator {

    /**
     * Maps input and output slots <b>against</b> the direction of the data flow.
     */
    private final SlotMapping slotMapping;

    private Operator inputOperator, outputOperator;

    /**
     * Wrap the given operators in a new instance (unless its a single operator),
     * thereby redirecting cut off connections through this subplan.
     */
    public static Operator wrap(Operator inputOperator, Operator outputOperator) {
        if (inputOperator == outputOperator) {
            return inputOperator;
        }

        final CompositeOperator commonParent = Operators.getCommonParent(inputOperator, outputOperator);
        // TODO: If the input operator does not have the common parent as parent, then it must be "terminal" in its parent.
        // TODO: If the output operator does not have the common parent as parent, then it must be "terminal" in its parent.

        Subplan newSubplan = new Subplan(inputOperator, outputOperator, commonParent);

        // Copy the interface of the input operator, steal its connections, and map the slots.
        InputSlot.mock(inputOperator, newSubplan);
        InputSlot.stealConnections(inputOperator, newSubplan);
        newSubplan.slotMapping.mapAll(inputOperator.getAllInputs(), newSubplan.inputSlots);

        // Copy the interface of the output operator, steal its connections, and map the slots.
        OutputSlot.mock(outputOperator, newSubplan);
        OutputSlot.stealConnections(outputOperator, newSubplan);
        newSubplan.slotMapping.mapAll(newSubplan.outputSlots, outputOperator.getAllOutputs());

        // Traverse through the subplan and become the parent of the operators.
        new PlanTraversal(true, false)
                .withCallback((operator, inputSlot, outputSlot) -> operator.setParent(newSubplan))
                .traverse(newSubplan.outputOperator);

        return newSubplan;
    }


    /**
     * Creates a new instance with the given operators.
     *
     * @see #wrap(Operator, Operator)
     */
    private Subplan(Operator inputOperator, Operator outputOperator, CompositeOperator parent) {
        super(inputOperator.getNumInputs(), outputOperator.getNumOutputs(), parent);

        this.inputOperator = inputOperator;
        this.outputOperator = outputOperator;
        this.slotMapping = new SlotMapping();

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
        if (this.isOwnerOf(subplanOutputSlot)) {
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

    @Override
    public void accept(PlanVisitor visitor) {
        visitor.visit(this);
    }

    // TODO: develop constructors/factory methods to deal with more than one input and output operator


    @Override
    public SlotMapping getSlotMappingFor(Operator child) {
        if (child.getParent() != this) {
            throw new IllegalArgumentException("Given operator is not a child of this subplan.");
        }

        if (child == this.inputOperator || child == this.outputOperator) {
            return this.slotMapping;
        }

        return null;
    }

    @Override
    public void replace(Operator oldOperator, Operator newOperator) {
        if (oldOperator == this.inputOperator) {
            this.slotMapping.replaceInputSlotMappings(oldOperator, newOperator);
            this.inputOperator = newOperator;
        }

        if (oldOperator == this.outputOperator) {
            this.slotMapping.replaceOutputSlotMappings(oldOperator, newOperator);
            this.outputOperator = newOperator;
        }
    }

}
