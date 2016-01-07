package org.qcri.rheem.core.mapping;

import org.qcri.rheem.core.plan.InputSlot;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.OutputSlot;

/**
 * An operator pattern matches to a class of operator instances.
 */
public class OperatorPattern<T extends Operator> implements Operator {

    private final String name;

    private final InputSlot[] inputs;

    private final OutputSlot[] outputs;

    private final Class<T> operatorClass;

    private final boolean isMatchSubclasses;

    public OperatorPattern(String name,
                           T exampleOperator,
                           boolean isMatchSubclasses) {
        this.name = name;
        this.inputs = new InputSlot[exampleOperator.getNumInputs()];
        for (int i = 0; i < this.inputs.length; i++) {
            this.inputs[i] = exampleOperator.getInput(i).copyFor(this);
        }
        this.outputs = new OutputSlot[exampleOperator.getNumOutputs()];
        for (int i = 0; i < this.outputs.length; i++) {
            this.outputs[i] = exampleOperator.getOutput(i).copyFor(this);
        }
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

    @Override
    public InputSlot[] getAllInputs() {
        return inputs;
    }

    @Override
    public OutputSlot[] getAllOutputs() {
        return outputs;
    }

    public String getName() {

        return name;
    }
}
