package org.qcri.rheem.core.plan;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A subplan encapsulates connected operators as a single operator.
 * <p><i>NB: So far, subplans must have exactly one input and one output operator whose input/output slots they
 * expose as their own. This design is likely to change soon.</i></p>
 */
public class Subplan implements Operator {


    private final Operator inputOperator, outputOperator;

    public Subplan(Operator inputOperator, Operator outputOperator) {
        this.inputOperator = inputOperator;
        this.outputOperator = outputOperator;
    }

    @Override
    public InputSlot[] getAllInputs() {
        return this.inputOperator.getAllInputs();
    }

    @Override
    public OutputSlot[] getAllOutputs() {
        return this.outputOperator.getAllOutputs();
    }

}
