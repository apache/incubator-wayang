package org.qcri.rheem.core.mapping;

import org.qcri.rheem.core.plan.rheemplan.*;

/**
 * An operator pattern matches to a class of operator instances.
 */
public class OperatorPattern<T extends Operator> extends OperatorBase {

    private final String name;

    private final Class<T> operatorClass;

    private final boolean isMatchSubclasses;

    /**
     * Creates a new instance.
     * @param name used to identify the new instance (e.g., in {@link SubplanMatch}es)
     * @param exampleOperator serves as template of the {@link Operator}s to match
     * @param isMatchSubclasses whether to match subclasses of the {@code exampleOperator}
     */
    public OperatorPattern(String name,
                           T exampleOperator,
                           boolean isMatchSubclasses) {

        super(exampleOperator.getNumInputs(), exampleOperator.getNumOutputs(),
                exampleOperator.isSupportingBroadcastInputs(),
                null);

        this.name = name;
        InputSlot.mock(exampleOperator, this);
        OutputSlot.mock(exampleOperator, this);

        this.operatorClass = (Class<T>) exampleOperator.getClass();
        this.isMatchSubclasses = isMatchSubclasses;
    }

    /**
     * Test whether this pattern matches a given operator.
     *
     * @param operator the operator to match or {@code null}, which represents the absence of an operator to match
     * @return whether the operator matches
     */
    public OperatorMatch match(Operator operator) {
        if (operator == null) return null;

        // Only match by the class so far.
        if (this.isMatchSubclasses ?
                this.operatorClass.isAssignableFrom(operator.getClass()) :
                this.operatorClass.equals(operator.getClass())) {
            this.checkSanity(operator);
            return new OperatorMatch(this, operator);
        }

        return null;
    }

    private void checkSanity(Operator operator) {
        if (this.getNumRegularInputs() != operator.getNumRegularInputs()) {
            throw new IllegalStateException(String.format("%s expected %d inputs, but matched %s with %d inputs.",
                    this, this.getNumRegularInputs(), operator, operator.getNumRegularInputs()));
        }
        if (this.getNumOutputs() != operator.getNumOutputs()) {
            throw new IllegalStateException("Matched an operator with different numbers of outputs.");
        }
    }


    public String getName() {
        return this.name;
    }

    @Override
    public <Payload, Return> Return accept(TopDownPlanVisitor<Payload, Return> visitor, OutputSlot<?> outputSlot, Payload payload) {
        throw new RuntimeException("Pattern does not accept visitors.");
    }

    @Override
    public String toString() {
        return String.format("%s[%d->%d, %s, id=%x]",
                this.getClass().getSimpleName(),
                this.getNumInputs(),
                this.getNumOutputs(),
                this.operatorClass.getSimpleName(),
                this.hashCode());
    }
}
