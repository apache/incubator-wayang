package org.qcri.rheem.core.plan;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * This operator encapsulates operators that are alternative to each other.
 */
public class OperatorAlternative extends OperatorBase {

    /**
     * All alternatives for this operator. Note that we deliberately do not use a {@link SlotMapping} at this point
     * because this can be achieved with a {@link Subplan}.
     */
    private List<Alternative> alternatives = new LinkedList<>();

    /**
     * Wraps an {@link Operator}:
     * <ol>
     * <li>Creates a new instance that mocks the interface (slots) of the given operator,</li>
     * <li>steals the connections from the given operator.</li>
     * <li>Moreover, the given operator is set up as the first alternative.</li>
     * </ol>
     *
     * @param operator operator to wrap
     * @param parent   parent of this operator or {@code null}
     */
    public OperatorAlternative(Operator operator, Operator parent) {
        super(operator.getNumInputs(), operator.getNumOutputs(), parent);
        InputSlot.mock(operator, this);
        InputSlot.stealConnections(operator, this);
        OutputSlot.mock(operator, this);
        OutputSlot.stealConnections(operator, this);
        addAlternative(operator);
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
    public void accept(PlanVisitor visitor) {
        visitor.visit(this);
    }

    public class Alternative implements Operator {

        private final SlotMapping slotMapping;

        private final Operator operator;

        private Alternative(Operator operator, SlotMapping slotMapping) {
            this.slotMapping = slotMapping;
            this.operator = operator;
            operator.setParent(this);
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
            if (!this.isOwnerOf(alternativeOutputSlot)) {
                throw new IllegalArgumentException("Cannot enter alternative: Output slot does not belong to this alternative.");
            }

            final OutputSlot<T> resolvedSlot = this.slotMapping.resolve(alternativeOutputSlot);
            if (resolvedSlot != null && resolvedSlot.getOwner().getParent() != this) {
                throw new IllegalStateException("Traced to an output slot whose owner is not a child of this alternative.");
            }
            return resolvedSlot;
        }

        public <T> InputSlot<T> exit(InputSlot<T> innerInputSlot) {
            if (innerInputSlot.getOwner().getParent() != OperatorAlternative.this) {
                throw new IllegalArgumentException("Trying to exit from an input slot that is not within this alternative.");
            }
            return this.slotMapping.resolve(innerInputSlot);
        }

        public OperatorAlternative exit(Operator innerOperator) {
            if (!isSource()) {
                throw new IllegalArgumentException("Cannot exit alternative: no input slot given and alternative is not a source.");
            }

            return innerOperator == this.operator ? OperatorAlternative.this : null;
        }

        @Override
        public InputSlot<?>[] getAllInputs() {
            return OperatorAlternative.this.getAllInputs();
        }

        @Override
        public OutputSlot<?>[] getAllOutputs() {
            return OperatorAlternative.this.getAllOutputs();
        }

        @Override
        public void accept(PlanVisitor visitor) {
            visitor.visit(OperatorAlternative.this);
        }

        @Override
        public Operator getParent() {
            return OperatorAlternative.this.getParent();
        }

        @Override
        public void setParent(Operator newParent) {
            throw new RuntimeException("Operation not supported. Use enclosing " + OperatorAlternative.class.getSimpleName());
        }

        @Override
        public boolean isOwnerOf(Slot<?> slot) {
            return OperatorAlternative.this.isOwnerOf(slot);
        }
    }

}
