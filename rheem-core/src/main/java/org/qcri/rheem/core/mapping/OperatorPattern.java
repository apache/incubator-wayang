package org.qcri.rheem.core.mapping;

import org.qcri.rheem.core.plan.*;

/**
 * An operator pattern matches to a class of operator instances.
 */
public class OperatorPattern<T extends Operator> extends OperatorBase {

    private final String name;

    private final Class<T> operatorClass;

    private final boolean isMatchSubclasses;

    public OperatorPattern(String name,
                           T exampleOperator,
                           boolean isMatchSubclasses) {

        super(exampleOperator.getNumInputs(), exampleOperator.getNumOutputs(), null);

        this.name = name;
        InputSlot.mock(exampleOperator, this);
        OutputSlot.mock(exampleOperator, this);

        this.operatorClass = (Class<T>) exampleOperator.getClass();
        this.isMatchSubclasses = isMatchSubclasses;
    }

    /**
     * Test whether this pattern matches a given operator.
     * @param operator the operator to match or {@code null}, which represents the absence of an operator to match
     * @return whether the operator matches
     */
    public OperatorMatch match(Operator operator) {
        if (operator == null) return null;
        if (this.isMatchSubclasses ?
                this.operatorClass.isAssignableFrom(operator.getClass()) :
                this.operatorClass.equals(operator.getClass())) {
            checkSanity(operator);
            return new OperatorMatch(this, operator); // todo
        }

        return null;
    }

    private void checkSanity(Operator operator) {
        if (this.getNumInputs() != operator.getNumInputs()) {
            throw new IllegalStateException("Matched an operator with different numbers of inputs.");
        }
        if (this.getNumOutputs() != operator.getNumOutputs()) {
            throw new IllegalStateException("Matched an operator with different numbers of outputs.");
        }
    }


    public String getName() {
        return name;
    }

    @Override
    public void accept(PlanVisitor visitor) {
        throw new RuntimeException("Pattern does not accept visitors.");
    }
}
