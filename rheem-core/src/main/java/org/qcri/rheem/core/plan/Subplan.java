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

    /**
     * Traverse the plan that this subplan is part of for sinks.
     *
     * @return all sinks that are reachable from this subplan
     */
    public Collection<Operator> getReachableSinks() {
        Set<Operator> allOperators = new HashSet<>();
        for (InputSlot inputSlot : getAllInputs()) {
            collectOperators(inputSlot.getOwner(), allOperators);
        }

        for (OutputSlot outputSlot : getAllOutputs()) {
            collectOperators(outputSlot.getOwner(), allOperators);
        }

        allOperators.removeIf(operator -> !operator.isSink());

        return allOperators;
    }

    /**
     * Collect the given operator and all operators that are reachable from it.
     *
     * @param operator     the operator to spread out the collecting from
     * @param allOperators a collector for any encountered operator
     */
    private void collectOperators(Operator operator, Set<Operator> allOperators) {
        if (operator == null || !allOperators.add(operator)) return;

        for (InputSlot inputSlot : operator.getAllInputs()) {
            collectOperators(inputSlot.getOwner(), allOperators);
        }

        for (OutputSlot outputSlot : operator.getAllOutputs()) {
            collectOperators(outputSlot.getOwner(), allOperators);
        }
    }
}
