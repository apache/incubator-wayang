package org.qcri.rheem.core.plan;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Traverse a plan.
 */
public class PlanTraversal {

    public Set<Operator> visitedOperators = new HashSet<>();

    /**
     * Traverse the plan by following any connected operators.
     *
     * @param operator the start point of the traversal
     * @return this instance
     */
    public PlanTraversal traverse(Operator operator) {
        if (visitedOperators.add(operator)) {
            traverseInputs(operator);
            traverseOutputs(operator);
        }

        return this;
    }

    /**
     * Override to control the traversal behavior.
     */
    protected void traverseInputs(Operator operator) {
        Arrays.stream(operator.getAllInputs())
                .map(InputSlot::getOccupant)
                .filter(outputSlot -> outputSlot != null)
                .map(OutputSlot::getOwner)
                .forEach(this::traverse);
    }

    /**
     * Override to control the traversal behavior.
     */
    protected void traverseOutputs(Operator operator) {
        Arrays.stream(operator.getAllOutputs())
                .map(OutputSlot::getOccupiedSlots)
                .flatMap(Collection::stream)
                .filter(inputSlot -> inputSlot != null)
                .map(InputSlot::getOwner)
                .forEach(this::traverse);
    }

    /**
     * Retrieve all traversed operators that fulfill a predicate.
     *
     * @param operatorPredicate the predicate to filter desired operators
     * @return previously traversed operators matching the predicated
     */
    public Collection<Operator> getTraversedNodesWith(Predicate<Operator> operatorPredicate) {
        return this.visitedOperators.stream().filter(operatorPredicate).collect(Collectors.toList());
    }

}
