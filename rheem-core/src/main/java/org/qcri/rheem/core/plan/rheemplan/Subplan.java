package org.qcri.rheem.core.plan.rheemplan;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A subplan encapsulates connected operators as a single operator.
 */
public class Subplan extends OperatorBase implements ActualOperator, CompositeOperator, OperatorContainer {

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

        final OperatorContainer commonContainer = Operators.getCommonContainer(inputOperator, outputOperator);
        // TODO: If the input operator does not have the common parent as parent, then it must be "terminal" in its parent.
        // TODO: If the output operator does not have the common parent as parent, then it must be "terminal" in its parent.

        Subplan newSubplan = new Subplan(inputOperator, outputOperator, commonContainer);

        // Copy the interface of the input operator, steal its connections, and map the slots.
        InputSlot.mock(inputOperator, newSubplan);
        InputSlot.stealConnections(inputOperator, newSubplan);
        newSubplan.slotMapping.mapAllUpsteam(inputOperator.getAllInputs(), newSubplan.inputSlots);

        // Copy the interface of the output operator, steal its connections, and map the slots.
        OutputSlot.mock(outputOperator, newSubplan);
        OutputSlot.stealConnections(outputOperator, newSubplan);
        newSubplan.slotMapping.mapAllUpsteam(newSubplan.outputSlots, outputOperator.getAllOutputs());

        // Traverse through the subplan and become the parent of the operators.
        new PlanTraversal(true, false)
                .withCallback((operator, inputSlot, outputSlot) -> operator.setContainer(newSubplan))
                .traverse(newSubplan.outputOperator);

        return newSubplan;
    }


    /**
     * Creates a new instance with the given operators.
     *
     * @see #wrap(Operator, Operator)
     */
    private Subplan(Operator inputOperator, Operator outputOperator, OperatorContainer container) {
        super(inputOperator.getNumInputs(), outputOperator.getNumOutputs(), container);

        this.inputOperator = inputOperator;
        this.outputOperator = outputOperator;
        this.slotMapping = new SlotMapping();

    }

    public SlotMapping getSlotMapping() {
        return slotMapping;
    }

    @Override
    public Operator getSource() {
        if (!isSource()) {
            throw new IllegalArgumentException("Cannot enter subplan: not a source");
        }

        return this.inputOperator;
    }

    @Override
    public <T> Collection<InputSlot<T>> followInput(InputSlot<T> inputSlot) {
        if (!this.isOwnerOf(inputSlot)) {
            throw new IllegalArgumentException("Cannot enter alternative: invalid input slot.");
        }

        final Collection<InputSlot<T>> resolvedSlots = this.slotMapping.resolveDownstream(inputSlot);
        for (InputSlot<T> resolvedSlot : resolvedSlots) {
            if (resolvedSlot != null && resolvedSlot.getOwner().getParent() != this) {
                final String msg = String.format("Cannot enter through: Owner of inner OutputSlot (%s) is not a child of this alternative (%s).",
                        Operators.collectParents(resolvedSlot.getOwner(), true),
                        Operators.collectParents(this, true));
                throw new IllegalStateException(msg);
            }
        }
        return resolvedSlots;
    }

    @Override
    public Operator getSink() {
        if (!isSink()) {
            throw new IllegalArgumentException("Cannot enter subplan: no output slot given and subplan is not a sink.");
        }

        return this.outputOperator;
    }

    @Override
    public <T> OutputSlot<T> traceOutput(OutputSlot<T> subplanOutputSlot) {
        // If this subplan is not a sink, we trace the given output slot via the slot mapping.
        if (!this.isOwnerOf(subplanOutputSlot)) {
            throw new IllegalArgumentException("Cannot enter subplan: Output slot does not belong to this subplan.");
        }

        final OutputSlot<T> resolvedSlot = this.slotMapping.resolveUpstream(subplanOutputSlot);
        if (resolvedSlot != null && resolvedSlot.getOwner().getParent() != this) {
            throw new IllegalStateException("Traced to an output slot whose owner is not a child of this subplan.");
        }
        return resolvedSlot;
    }

    @Override
    public CompositeOperator toOperator() {
        return this;
    }

    public <T> InputSlot<T> exit(InputSlot<T> innerInputSlot) {
        if (innerInputSlot.getOwner().getParent() != this) {
            throw new IllegalArgumentException("Trying to exit from an input slot that is not within this subplan.");
        }
        return this.slotMapping.resolveUpstream(innerInputSlot);
    }

    public Subplan exit(Operator innerOperator) {
        if (!isSource()) {
            throw new IllegalArgumentException("Cannot exit subplan: no input slot given and subplan is not a source.");
        }

        return innerOperator == this.inputOperator ? this : null;
    }


    @Override
    public <Payload, Return> Return accept(TopDownPlanVisitor<Payload, Return> visitor, OutputSlot<?> outputSlot, Payload payload) {
        return visitor.visit(this, outputSlot, payload);
    }

//    @Override
//    public <Payload, Return> Return accept(BottomUpPlanVisitor<Payload, Return> visitor, InputSlot<?> inputSlot, Payload payload) {
//        return visitor.visit(this, inputSlot, payload);
//    }

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

    @Override
    public <T> InputSlot<T> traceInput(InputSlot<T> inputSlot) {
        if (inputSlot.getOwner() != this) {
            throw new IllegalArgumentException("InputSlot does not belong to this Operator.");
        }
        return this.slotMapping.resolveUpstream(inputSlot);
    }

    @Override
    public <T> Collection<OutputSlot<T>> followOutput(OutputSlot<T> outputSlot) {
        if (outputSlot.getOwner().getContainer() != this) {
            throw new IllegalArgumentException("OutputSlot does not belong to this Operator.");
        }
        return this.slotMapping.resolveDownstream(outputSlot);
    }

    /**
     * Collect {@link Operator}s within this instance that are connected to an outer {@link OutputSlot}.
     *
     * @return the collected {@link Operator}s
     */
    public Collection<Operator> collectOutputOperators() {
        if (this.isSink()) {
            return Collections.singleton(this.getSink());
        }
        return Arrays.stream(getAllOutputs())
                .map(this.slotMapping::resolveUpstream)
                .filter(Objects::nonNull)
                .map(OutputSlot::getOwner)
                .collect(Collectors.toList());
    }

    /**
     * Collect {@link Operator}s within this instance that are connected to an outer {@link InputSlot}.
     *
     * @return the collected {@link Operator}s
     */
    public Collection<Operator> collectInputOperators() {
        if (this.isSource()) {
            return Collections.singleton(this.getSource());
        }

        return Arrays.stream(getAllInputs())
                .flatMap(inputSlot -> this.slotMapping.resolveDownstream(inputSlot).stream())
                .map(InputSlot::getOwner)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<CardinalityEstimator> getCardinalityEstimator(final int outputIndex,
                                                                  final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return CompositeCardinalityEstimator.createFor(this, outputIndex, configuration);
    }

    @Override
    public CardinalityPusher getCardinalityPusher(
            final Configuration configuration,
            final Map<OutputSlot<?>, CardinalityEstimate> cache) {
        return CompositeCardinalityPusher.createFor(this, configuration, cache);
    }
}
