package org.qcri.rheem.core.plan.rheemplan;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityPusher;
import org.qcri.rheem.core.optimizer.cardinality.OperatorAlternativeCardinalityPusher;
import org.qcri.rheem.core.util.RheemCollections;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This operator encapsulates operators that are alternative to each other.
 * <p>Alternatives and their interfaces (i.e., {@link OutputSlot}s and {@link InputSlot}s) are matched via their
 * input/output indices.</p>
 */
public class OperatorAlternative extends OperatorBase implements CompositeOperator {

    /**
     * All alternatives for this operator. Note that we deliberately do not use a {@link SlotMapping} at this point
     * because this can be achieved with a {@link Subplan}.
     */
    private List<Alternative> alternatives = new LinkedList<>();

    public static OperatorAlternative wrap(Operator startOperator, Operator endOperator) {
        if (startOperator != endOperator) {
            // TODO
            throw new UnsupportedOperationException("Different operators in a match currently not supported.");
        }

        return wrap(startOperator);
    }

    /**
     * Wraps an {@link Operator}:
     * <ol>
     * <li>Creates a new instance that mocks the interface (slots) of the given operator,</li>
     * <li>steals the connections from the given operator,</li>
     * <li>adapts its parent and becomes its new parent.</li>
     * <li>Moreover, the given operator is set up as the first alternative.</li>
     * </ol>
     *
     * @param operator operator to wrap
     */
    public static OperatorAlternative wrap(Operator operator) {
        // Test whether the operator is not already an alternative.
        final OperatorContainer container = operator.getContainer();
        if (container != null && container.toOperator().isAlternative() && container.getContainedOperators().size() == 1) {
            return (OperatorAlternative) container.toOperator();
        }

        OperatorAlternative operatorAlternative = operator.isLoopHead() ?
                new LoopHeadAlternative((LoopHeadOperator) operator) :
                new OperatorAlternative(operator);
        InputSlot.mock(operator, operatorAlternative);
        OutputSlot.mock(operator, operatorAlternative);

        Alternative alternative = operatorAlternative.addAlternative(operator);
        if (container != null) {
            operatorAlternative.setContainer(container);
            container.noteReplaced(operator, alternative);
        }
        return operatorAlternative;
    }

    /**
     * Creates a new instance with the same number of inputs and outputs and the same parent as the given operator.
     */
    protected OperatorAlternative(Operator operator) {
        super(operator.getNumInputs(), operator.getNumOutputs(), false);
    }

    public List<Alternative> getAlternatives() {
        return Collections.unmodifiableList(this.alternatives);
    }

    /**
     * Adds an {@link Alternative} to this instance.
     *
     * @param alternativeOperator either an {@link ElementaryOperator} or a {@link Subplan}; in the latter case, the
     *                            {@link Subplan} will be "unpacked" into the new {@link Alternative}
     */
    public Alternative addAlternative(Operator alternativeOperator) {
        Alternative alternative = this.createAlternative();
        if (alternativeOperator.isSubplan()) {
            OperatorContainers.move((Subplan) alternativeOperator, alternative);
        } else {
            assert alternativeOperator.isElementary();
            OperatorContainers.wrap(alternativeOperator, alternative);
        }
        this.alternatives.add(alternative);
        return alternative;
    }

    protected Alternative createAlternative() {
        return new Alternative();
    }

    @Override
    public <Payload, Return> Return accept(TopDownPlanVisitor<Payload, Return> visitor, OutputSlot<?> outputSlot, Payload payload) {
        return visitor.visit(this, outputSlot, payload);
    }

    @Override
    public void noteReplaced(Operator oldOperator, Operator newOperator) {
    }

    @Override
    public void propagateOutputCardinality(int outputIndex,
                                           OptimizationContext.OperatorContext operatorContext,
                                           OptimizationContext targetContext) {
        super.propagateOutputCardinality(outputIndex, operatorContext, targetContext);
        this.getAlternatives().forEach(alternative -> alternative.propagateOutputCardinality(outputIndex, operatorContext));
    }

    @Override
    public void propagateInputCardinality(int inputIndex, OptimizationContext.OperatorContext operatorContext) {
        super.propagateInputCardinality(inputIndex, operatorContext);
        this.getAlternatives().forEach(alternative -> alternative.propagateInputCardinality(inputIndex, operatorContext));
    }

    @Override
    public <T> Set<OutputSlot<T>> collectMappedOutputSlots(OutputSlot<T> output) {
        return Stream.concat(
                Stream.of(output),
                this.alternatives.stream().flatMap(alternative -> this.streamMappedOutputSlots(alternative, output))
        ).collect(Collectors.toSet());
    }

    private <T> Stream<OutputSlot<T>> streamMappedOutputSlots(
            OperatorAlternative.Alternative alternative,
            OutputSlot<T> output) {
        final OutputSlot<T> innerOutput = alternative.traceOutput(output);
        return innerOutput == null ?
                Stream.empty() :
                innerOutput.getOwner().collectMappedOutputSlots(innerOutput).stream();
    }

    @Override
    public <T> Set<InputSlot<T>> collectMappedInputSlots(InputSlot<T> input) {
        return Stream.concat(
                Stream.of(input),
                this.alternatives.stream().flatMap(alternative -> this.streamMappedInputSlots(alternative, input))
        ).collect(Collectors.toSet());
    }

    private <T> Stream<InputSlot<T>> streamMappedInputSlots(
            OperatorAlternative.Alternative alternative,
            InputSlot<T> input) {
        final Collection<InputSlot<T>> innerInputs = alternative.followInput(input);
        return Stream.concat(
                Stream.of(input),
                innerInputs.stream().flatMap(innerInput -> innerInput.getOwner().collectMappedInputSlots(innerInput).stream())
        );
    }

    @Override
    public CardinalityPusher getCardinalityPusher(final Configuration configuration) {
        return new OperatorAlternativeCardinalityPusher(this, configuration);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<OperatorContainer> getContainers() {
        return (Collection<OperatorContainer>) (Collection) this.alternatives;
    }

    @Override
    public String toString() {
        return String.format("%s[%dx ~%s, %x]",
                this.getSimpleClassName(),
                this.alternatives.size(),
                RheemCollections.getAnyOptional(this.alternatives).orElse(null),
                this.hashCode());
    }

    /**
     * Represents an alternative subplan for the enclosing {@link OperatorAlternative}.
     */
    public class Alternative implements OperatorContainer {

        /**
         * Maps the slots of the enclosing {@link OperatorAlternative} with this instance.
         */
        private final SlotMapping slotMapping = new SlotMapping();

        /**
         * Source/sink {@link Operator} in this instance. Should only be set if the surrounding {@link OperatorAlternative}
         * is a source/sink.
         */
        private Operator source, sink;


        private Alternative() {
        }

        @Override
        public SlotMapping getSlotMapping() {
            return this.slotMapping;
        }

        @Override
        public Operator getSink() {
            return this.sink;
        }

        @Override
        public void setSink(Operator innerSink) {
            this.sink = innerSink;
        }

        @Override
        public <T> OutputSlot<T> traceOutput(OutputSlot<T> alternativeOutputSlot) {
            // If this alternative is not a sink, we trace the given output slot via the slot mapping.
            if (!OperatorAlternative.this.isOwnerOf(alternativeOutputSlot)) {
                throw new IllegalArgumentException("Cannot enter alternative: Output slot does not belong to this alternative.");
            }

            final OutputSlot<T> resolvedSlot = this.slotMapping.resolveUpstream(alternativeOutputSlot);
            if (resolvedSlot != null && resolvedSlot.getOwner().getParent() != OperatorAlternative.this) {
                final String msg = String.format("Cannot enter through: Owner of inner OutputSlot (%s) is not a child of this alternative (%s).",
                        Operators.collectParents(resolvedSlot.getOwner(), true),
                        Operators.collectParents(OperatorAlternative.this, true));
                throw new IllegalStateException(msg);
            }
            return resolvedSlot;
        }

        @Override
        public OperatorAlternative toOperator() {
            return OperatorAlternative.this;
        }

        @Override
        public Operator getSource() {
            return this.source;
        }

        @Override
        public void setSource(Operator innerSource) {
            this.source = innerSource;
        }

        @Override
        public <T> Collection<InputSlot<T>> followInput(InputSlot<T> inputSlot) {
            if (!OperatorAlternative.this.isOwnerOf(inputSlot)) {
                throw new IllegalArgumentException("Cannot enter alternative: invalid input slot.");
            }

            final Collection<InputSlot<T>> resolvedSlots = this.slotMapping.resolveDownstream(inputSlot);
            for (InputSlot<T> resolvedSlot : resolvedSlots) {
                if (resolvedSlot != null && resolvedSlot.getOwner().getParent() != OperatorAlternative.this) {
                    final String msg = String.format("Cannot enter through: Owner of inner OutputSlot (%s) is not a child of this alternative (%s).",
                            Operators.collectParents(resolvedSlot.getOwner(), true),
                            Operators.collectParents(OperatorAlternative.this, true));
                    throw new IllegalStateException(msg);
                }
            }
            return resolvedSlots;
        }

        @Override
        public <T> InputSlot<T> traceInput(InputSlot<T> inputSlot) {
            if (inputSlot.getOwner().getContainer() != this) {
                throw new IllegalArgumentException("Cannot trace input slot: does not belong to this alternative.");
            }

            final InputSlot<T> tracedInput = this.slotMapping.resolveUpstream(inputSlot);
            assert tracedInput == null || inputSlot.getOccupant() == null : String.format(
                    "%s has both the occupant %s and the outer mapped input %s.",
                    inputSlot, inputSlot.getOccupant(), tracedInput
            );
            return tracedInput;
        }

        @Override
        public <T> Collection<OutputSlot<T>> followOutput(OutputSlot<T> outputSlot) {
            if (outputSlot.getOwner().getContainer() != this) {
                throw new IllegalArgumentException("OutputSlot does not belong to this Alternative.");
            }
            return this.slotMapping.resolveDownstream(outputSlot);
        }

        public OperatorAlternative getOperatorAlternative() {
            return OperatorAlternative.this;
        }

        @Override
        public String toString() {
            return String.format("%s[%s]", this.getClass().getSimpleName(), this.getContainedOperators());
        }
    }
}
