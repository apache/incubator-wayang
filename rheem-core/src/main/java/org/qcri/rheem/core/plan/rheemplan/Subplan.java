package org.qcri.rheem.core.plan.rheemplan;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityPusher;
import org.qcri.rheem.core.optimizer.cardinality.CompositeCardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.CompositeCardinalityPusher;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A subplan encapsulates connected operators as a single operator.
 */
public class Subplan extends OperatorBase implements ActualOperator, CompositeOperator, OperatorContainer {

    /**
     * Maps input and output slots <b>against</b> the direction of the data flow.
     */
    private final SlotMapping slotMapping;

    /**
     * If this instance is a source or a sink, then the encapsulated source/sink {@link Operator}s are stored here.
     * Otherwise {@code null}.
     */
    private Operator source, sink;

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

        return new Subplan(
                Arrays.asList(inputOperator.getAllInputs()),
                Arrays.asList(outputOperator.getAllOutputs()),
                commonContainer
        );

//        // Copy the interface of the input operator, steal its connections, and map the slots.
//        InputSlot.mock(inputOperator, newSubplan, false);
//        InputSlot.stealConnections(inputOperator, newSubplan);
//        newSubplan.slotMapping.mapAllUpsteam(inputOperator.getAllInputs(), newSubplan.inputSlots);
//
//        // Copy the interface of the output operator, steal its connections, and map the slots.
//        OutputSlot.mock(outputOperator, newSubplan);
//        OutputSlot.stealConnections(outputOperator, newSubplan);
//        newSubplan.slotMapping.mapAllUpsteam(newSubplan.outputSlots, outputOperator.getAllOutputs());
//
//        // Traverse through the subplan and become the parent of the operators.
//        PlanTraversal.upstream()
//                .withCallback((operator, inputSlot, outputSlot) -> operator.setContainer(newSubplan))
//                .traverse(newSubplan.outputOperator);
//
//        return newSubplan;
    }


    /**
     * Creates a new instance with the given operators.
     *
     * @see #wrap(Operator, Operator)
     */
    private Subplan(List<InputSlot<?>> inputSlots, List<OutputSlot<?>> outputSlots, OperatorContainer container) {
        super(inputSlots.size(), outputSlots.size(), false, container);
        this.slotMapping = new SlotMapping();

        // Copy and steal the inputSlots.
        for (int inputIndex = 0; inputIndex < inputSlots.size(); inputIndex++) {
            InputSlot<?> innerInput = inputSlots.get(inputIndex);
            final InputSlot<?> outerInput = innerInput.copyFor(this);
            this.inputSlots[inputIndex] = outerInput;
            outerInput.unchecked().stealOccupant(innerInput.unchecked());
            this.slotMapping.mapUpstream(innerInput, outerInput);
        }

        // Copy and steal the outputSlots.
        for (int outputIndex = 0; outputIndex < outputSlots.size(); outputIndex++) {
            OutputSlot<?> innerOutput = outputSlots.get(outputIndex);
            final OutputSlot<?> outerOutput = innerOutput.copyFor(this);
            this.outputSlots[outputIndex] = outerOutput;
            outerOutput.unchecked().stealOccupiedSlots(innerOutput.unchecked());
            this.slotMapping.mapUpstream(outerOutput, innerOutput);
        }

        // Mark all contained Operators and detect sources and sinks.
        PlanTraversal.fanOut()
                .followingInputsIf(input -> !inputSlots.contains(input))
                .followingOutputsIf(output -> !outputSlots.contains(output))
                .withCallback(operator -> {
                    operator.setContainer(this);
                    if (operator.isSink()) {
                        Validate.isTrue(this.isSink(), "Detected sink %s in non-sink %s.", operator, this);
                        Validate.isTrue(this.sink == null, "At least two sinks %s and %s in %s.", operator, this.sink, this);
                        this.sink = operator;
                    }
                    if (operator.isSource()) {
                        Validate.isTrue(this.isSource(), "Detected source %s in non-source %s.", operator, this);
                        Validate.isTrue(this.source == null, "At least two sources %s and %s in %s.", operator, this.source, this);
                        this.source = operator;
                    }
                })
                .traverse(Stream.concat(inputSlots.stream(), outputSlots.stream()).map(Slot::getOwner));

        Validate.isTrue((this.source == null) ^ this.isSource());
        Validate.isTrue((this.sink == null) ^ this.isSink());
    }

    @Override
    public SlotMapping getSlotMapping() {
        return this.slotMapping;
    }

    @Override
    public Operator getSource() {
        if (!this.isSource()) {
            throw new IllegalArgumentException("Cannot enter subplan: not a source");
        }

        return this.source;
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
        if (!this.isSink()) {
            throw new IllegalArgumentException("Cannot enter subplan: no output slot given and subplan is not a sink.");
        }

        return this.sink;
    }

    @Override
    @SuppressWarnings("null")
    public <T> OutputSlot<T> traceOutput(OutputSlot<T> subplanOutputSlot) {
        // If this subplan is not a sink, we trace the given output slot via the slot mapping.
        Validate.isTrue(
                this.isOwnerOf(subplanOutputSlot),
                "Cannot enter subplan: %s does not belong to %s.",
                subplanOutputSlot, this
        );

        final OutputSlot<T> resolvedSlot = this.slotMapping.resolveUpstream(subplanOutputSlot);
        Validate.isTrue(
                resolvedSlot == null || resolvedSlot.getOwner().getParent() == this,
                "Traced to %s to %s whose owner %s is contained in %s instead of %s.",
                subplanOutputSlot, resolvedSlot, resolvedSlot.getOwner(), resolvedSlot.getOwner().getContainer(), this
        );
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

        return this.slotMapping;
    }

    @Override
    public void replace(Operator oldOperator, Operator newOperator) {
        this.slotMapping.replaceInputSlotMappings(oldOperator, newOperator);
        this.slotMapping.replaceOutputSlotMappings(oldOperator, newOperator);
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
        return Arrays.stream(this.getAllOutputs())
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

        return Arrays.stream(this.getAllInputs())
                .flatMap(inputSlot -> this.slotMapping.resolveDownstream(inputSlot).stream())
                .map(InputSlot::getOwner)
                .collect(Collectors.toList());
    }

    @Override
    public <T> Set<OutputSlot<T>> collectMappedOutputSlots(OutputSlot<T> output) {
        final OutputSlot<T> innerOutput = this.traceOutput(output);
        return Stream.concat(
                Stream.of(output),
                innerOutput == null ?
                        Stream.empty() :
                        innerOutput.getOwner().collectMappedOutputSlots(innerOutput).stream()
        ).collect(Collectors.toSet());
    }

    @Override
    public Optional<CardinalityEstimator> getCardinalityEstimator(final int outputIndex,
                                                                  final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return CompositeCardinalityEstimator.createFor(this, outputIndex, configuration);
    }

    @Override
    public CardinalityPusher getCardinalityPusher(
            final Configuration configuration) {
        return CompositeCardinalityPusher.createFor(this, configuration);
    }

}
