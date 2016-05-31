package org.qcri.rheem.core.plan.rheemplan;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.AggregatingCardinalityPusher;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityPusher;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This operator encapsulates operators that are alternative to each other.
 * <p>TODO: Alternatives and their interfaces (i.e., {@link OutputSlot}s and {@link InputSlot}s) are matched via their
 * input/output indices.</p>
 */
public class OperatorAlternative extends OperatorBase implements CompositeOperator {

    /**
     * All alternatives for this operator. Note that we deliberately do not use a {@link SlotMapping} at this point
     * because this can be achieved with a {@link Subplan}.
     */
    private List<Alternative> alternatives = new LinkedList<>();

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
        OperatorAlternative operatorAlternative =
                operator.isLoopHead() ?
                        new LoopHeadAlternative((LoopHeadOperator) operator) :
                        new OperatorAlternative(operator);

        InputSlot.mock(operator, operatorAlternative, false);
        InputSlot.stealConnections(operator, operatorAlternative);

        OutputSlot.mock(operator, operatorAlternative);
        OutputSlot.stealConnections(operator, operatorAlternative);

        operatorAlternative.addAlternative(operator);

        return operatorAlternative;
    }

    /**
     * Creates a new instance with the same number of inputs and outputs and the same parent as the given operator.
     */
    protected OperatorAlternative(Operator operator) {
        super(operator.getNumInputs(), operator.getNumOutputs(), false, operator.getContainer());
    }

    public List<Alternative> getAlternatives() {
        return Collections.unmodifiableList(this.alternatives);
    }

    public void addAlternative(Operator alternative) {
        this.addAlternative(new Alternative(alternative, SlotMapping.wrap(alternative, this)));
    }

    private void addAlternative(Alternative alternative) {
        this.alternatives.add(alternative);
    }

    @Override
    public <Payload, Return> Return accept(TopDownPlanVisitor<Payload, Return> visitor, OutputSlot<?> outputSlot, Payload payload) {
        return visitor.visit(this, outputSlot, payload);
    }

//    @Override
//    public <Payload, Return> Return accept(BottomUpPlanVisitor<Payload, Return> visitor, InputSlot<?> inputSlot, Payload payload) {
//        return visitor.visit(this, inputSlot, payload);
//    }

    @Override
    public SlotMapping getSlotMappingFor(Operator child) {
        if (child.getParent() != this) {
            throw new IllegalArgumentException("Given operator is not a child of this alternative.");
        }

        return this.alternatives.stream()
                .filter(alternative -> alternative.getOperator() == child)
                .findFirst()
                .map(Alternative::getSlotMapping)
                .orElseThrow(() -> new RuntimeException("Could not find alternative for child."));
    }

    @Override
    public void replace(Operator oldOperator, Operator newOperator) {
        Operators.assertEqualInputs(oldOperator, newOperator);
        Operators.assertEqualOutputs(oldOperator, newOperator);

        for (int i = 0; i < this.alternatives.size(); i++) {
            final Alternative alternative = this.alternatives.get(i);
            if (alternative.getOperator() == oldOperator) {
                final SlotMapping slotMapping = alternative.getSlotMapping();
                slotMapping.replaceInputSlotMappings(oldOperator, newOperator);
                slotMapping.replaceOutputSlotMappings(oldOperator, newOperator);
                alternative.operator = newOperator;
            }
        }
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
    public String toString() {
        return String.format("%s[%dx ~%s, %x]",
                this.getSimpleClassName(),
                this.alternatives.size(),
                this.alternatives.get(0).getOperator(),
                this.hashCode());
    }

    /**
     * Represents an alternative subplan for the enclosing {@link OperatorAlternative}.
     */
    public class Alternative implements OperatorContainer {

        /**
         * Maps the slots of the enclosing {@link OperatorAlternative} with the enclosed {@link #operator}.
         */
        private final SlotMapping slotMapping;

        /**
         * The operator/subplan encapsulated by this {@link OperatorAlternative.Alternative}.
         */
        private Operator operator;

        private Alternative(Operator operator, SlotMapping slotMapping) {
            this.slotMapping = slotMapping;
            this.operator = operator;
            operator.setContainer(this);
        }

        @Override
        public SlotMapping getSlotMapping() {
            return this.slotMapping;
        }

        public Operator getOperator() {
            return this.operator;
        }

        @Override
        public Operator getSink() {
            if (!OperatorAlternative.this.isSink()) {
                throw new IllegalArgumentException("Cannot enter alternative: no output slot given and alternative is not a sink.");
            }

            return this.operator;
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
            if (!OperatorAlternative.this.isSource()) {
                throw new IllegalStateException("Cannot enter alternative: not a source.");
            }

            return this.operator;
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
            if (inputSlot.getOccupant() != null) {
                throw new IllegalStateException("Cannot trace an InputSlot that has an occupant.");
            }

            if (inputSlot.getOwner().getContainer() != this) {
                throw new IllegalArgumentException("Cannot trace input slot: does not belong to this alternative.");
            }

            return this.slotMapping.resolveUpstream(inputSlot);
        }

        @Override
        public <T> Collection<OutputSlot<T>> followOutput(OutputSlot<T> outputSlot) {
            if (outputSlot.getOwner().getContainer() != this) {
                throw new IllegalArgumentException("OutputSlot does not belong to this Alternative.");
            }
            return this.slotMapping.resolveDownstream(outputSlot);
        }

        public <T> InputSlot<T> exit(InputSlot<T> innerInputSlot) {
            if (innerInputSlot.getOwner().getParent() != OperatorAlternative.this) {
                throw new IllegalArgumentException("Trying to exit from an input slot that is not within this alternative.");
            }
            return this.slotMapping.resolveUpstream(innerInputSlot);
        }

        public OperatorAlternative getOperatorAlternative() {
            return OperatorAlternative.this;
        }

        public OperatorAlternative exit(Operator innerOperator) {
            if (!OperatorAlternative.this.isSource()) {
                throw new IllegalArgumentException("Cannot exit alternative: no input slot given and alternative is not a source.");
            }

            return innerOperator == this.operator ? OperatorAlternative.this : null;
        }

    }

    @Override
    public CardinalityPusher getCardinalityPusher(final Configuration configuration) {
        return new AggregatingCardinalityPusher(this, configuration);
    }
}
