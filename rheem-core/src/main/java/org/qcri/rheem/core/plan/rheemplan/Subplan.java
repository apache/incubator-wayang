package org.qcri.rheem.core.plan.rheemplan;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityPusher;
import org.qcri.rheem.core.optimizer.cardinality.SubplanCardinalityPusher;

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

        return wrap(
                Arrays.asList(inputOperator.getAllInputs()),
                Arrays.asList(outputOperator.getAllOutputs()),
                commonContainer
        );
    }

    /**
     * Creates a new instance with the given operators. Initializes the {@link InputSlot}s and {@link OutputSlot}s,
     * steals existing connections, initializes the {@link #slotMapping}, and sets as inner {@link Operator}s' parent.
     */
    public static Subplan wrap(List<InputSlot<?>> inputs, List<OutputSlot<?>> outputs, OperatorContainer container) {
        return new Subplan(inputs, outputs, container);
    }

    /**
     * Creates a new instance with the given operators. Initializes the {@link InputSlot}s and {@link OutputSlot}s,
     * steals existing connections, initializes the {@link #slotMapping}, and sets as inner {@link Operator}s' parent.
     *
     * @see #wrap(Operator, Operator)
     * @see #wrap(List, List, OperatorContainer)
     */
    protected Subplan(List<InputSlot<?>> inputs, List<OutputSlot<?>> outputs, OperatorContainer container) {
        super(inputs.size(), outputs.size(), false, container);
        this.slotMapping = new SlotMapping();

        // Copy and steal the inputSlots.
        for (int inputIndex = 0; inputIndex < inputs.size(); inputIndex++) {
            InputSlot<?> innerInput = inputs.get(inputIndex);
            final InputSlot<?> outerInput = innerInput.copyFor(this);
            this.inputSlots[inputIndex] = outerInput;
            outerInput.unchecked().stealOccupant(innerInput.unchecked());
            this.slotMapping.mapUpstream(innerInput, outerInput);
        }

        // Copy and steal the outputSlots.
        for (int outputIndex = 0; outputIndex < outputs.size(); outputIndex++) {
            OutputSlot<?> innerOutput = outputs.get(outputIndex);
            final OutputSlot<?> outerOutput = innerOutput.copyFor(this);
            this.outputSlots[outputIndex] = outerOutput;
            outerOutput.unchecked().stealOccupiedSlots(innerOutput.unchecked());
            this.slotMapping.mapUpstream(outerOutput, innerOutput);
        }


        // Mark all contained Operators and detect sources and sinks.
        final Set<InputSlot<?>> inputSet = new HashSet<>(inputs);
        final Set<OutputSlot<?>> outputSet = new HashSet<>(outputs);
        PlanTraversal.fanOut()
                .followingInputsIf(input -> !inputSet.contains(input))
                .followingOutputsIf(output -> !outputSet.contains(output))
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
                .traverse(Stream.concat(inputSet.stream(), outputSet.stream()).map(Slot::getOwner));

        // Sanity checks.
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
    public boolean isSink() {
        return this.getNumOutputs() == 0;
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
        assert inputSlot.getOwner().getContainer() == this : String.format("%s is not encapsulated in %s.", inputSlot, this);
        return this.slotMapping.resolveUpstream(inputSlot);
    }

    @Override
    public <T> Collection<OutputSlot<T>> followOutput(OutputSlot<T> outputSlot) {
        assert outputSlot.getOwner().getContainer() == this : String.format("%s does not belong to %s.", outputSlot, this);
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
    public CardinalityPusher getCardinalityPusher(
            final Configuration configuration) {
        return SubplanCardinalityPusher.createFor(this, configuration);
    }

    @Override
    public void propagateInputCardinality(int inputIndex, OptimizationContext.OperatorContext operatorContext) {
        OperatorContainer.super.propagateInputCardinality(inputIndex, operatorContext);
    }

    @Override
    public void propagateOutputCardinality(int outputIndex, OptimizationContext.OperatorContext operatorContext) {
        OperatorContainer.super.propagateOutputCardinality(outputIndex, operatorContext);
    }
}
