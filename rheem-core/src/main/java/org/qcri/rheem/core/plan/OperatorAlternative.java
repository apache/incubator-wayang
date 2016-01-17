package org.qcri.rheem.core.plan;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * This operator encapsulates operators that are alternative to each other.
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
        OperatorAlternative operatorAlternative = new OperatorAlternative(operator);

        InputSlot.mock(operator, operatorAlternative);
        InputSlot.stealConnections(operator, operatorAlternative);

        OutputSlot.mock(operator, operatorAlternative);
        OutputSlot.stealConnections(operator, operatorAlternative);

        final CompositeOperator parent = operator.getParent();
        if (Objects.nonNull(parent)) {
            parent.replace(operator, operatorAlternative);
        }

        operatorAlternative.addAlternative(operator);

        return operatorAlternative;
    }

    /**
     * Creates a new instance with the same number of inputs and outputs and the same parent as the given operator.
     */
    private OperatorAlternative(Operator operator) {
        super(operator.getNumInputs(), operator.getNumOutputs(), operator.getParent());
    }

    public List<Alternative> getAlternatives() {
        return Collections.unmodifiableList(this.alternatives);
    }

    public void addAlternative(Operator alternative) {
        addAlternative(new Alternative(alternative, SlotMapping.wrap(alternative, this)));
    }

    private void addAlternative(Alternative alternative) {
        this.alternatives.add(alternative);
    }

    @Override
    public <Payload, Return> Return accept(PlanVisitor<Payload, Return> visitor, OutputSlot<?> outputSlot, Payload payload) {
        return visitor.visit(this, outputSlot, payload);
    }

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

    /**
     * Represents an alternative subplan for the enclosing {@link OperatorAlternative}.
     */
    public class Alternative {

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
            operator.setParent(OperatorAlternative.this);
        }

        public SlotMapping getSlotMapping() {
            return slotMapping;
        }

        public Operator getOperator() {
            return operator;
        }

        /**
         * Enter this alternative. This alternative needs to be a sink.
         *
         * @return the sink operator within this alternative
         */
        public Operator enter() {
            if (!isSink()) {
                throw new IllegalArgumentException("Cannot enter alternative: no output slot given and alternative is not a sink.");
            }

            return this.operator;
        }

        /**
         * Enter this alternative by following one of its output slots.
         *
         * @param alternativeOutputSlot an output slot of this alternative
         * @return the output within the alternative that is connected to the given output slot
         */
        public <T> OutputSlot<T> enter(OutputSlot<T> alternativeOutputSlot) {
            // If this alternative is not a sink, we trace the given output slot via the slot mapping.
            if (!OperatorAlternative.this.isOwnerOf(alternativeOutputSlot)) {
                throw new IllegalArgumentException("Cannot enter alternative: Output slot does not belong to this alternative.");
            }

            final OutputSlot<T> resolvedSlot = this.slotMapping.resolve(alternativeOutputSlot);
            if (resolvedSlot != null && resolvedSlot.getOwner().getParent() != OperatorAlternative.this) {
                final String msg = String.format("Cannot enter through: Owner of inner OutputSlot (%s) is not a child of this alternative (%s).",
                        Operators.collectParents(resolvedSlot.getOwner(), true),
                        Operators.collectParents(OperatorAlternative.this, true));
                throw new IllegalStateException(msg);
            }
            return resolvedSlot;
        }

        public <T> InputSlot<T> exit(InputSlot<T> innerInputSlot) {
            if (innerInputSlot.getOwner().getParent() != OperatorAlternative.this) {
                throw new IllegalArgumentException("Trying to exit from an input slot that is not within this alternative.");
            }
            return this.slotMapping.resolve(innerInputSlot);
        }

        public OperatorAlternative getOperatorAlternative() {
            return OperatorAlternative.this;
        }

        public OperatorAlternative exit(Operator innerOperator) {
            if (!isSource()) {
                throw new IllegalArgumentException("Cannot exit alternative: no input slot given and alternative is not a source.");
            }

            return innerOperator == this.operator ? OperatorAlternative.this : null;
        }

    }

}
