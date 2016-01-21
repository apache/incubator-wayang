package org.qcri.rheem.core.plan;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Traverse a plan. In each instance, every operator will be traversed only once.
 */
public class PlanTraversal {

    public Set<Operator> visitedOperators = new HashSet<>();

    private final boolean isFollowInputs, isFollowOutputs;

    private Callback traversalCallback = null;

    public PlanTraversal(boolean isFollowInputs, boolean isFollowOutputs) {
        this.isFollowInputs = isFollowInputs;
        this.isFollowOutputs = isFollowOutputs;
    }

    public PlanTraversal withCallback(Callback traversalCallback) {
        this.traversalCallback = traversalCallback;
        return this;
    }

    /**
     * Traversing as with {@link #traverse(Operator, InputSlot, OutputSlot)} for every operator.
     */
    public PlanTraversal traverse(Collection<? extends Operator> operators) {
        operators.forEach(this::traverse);
        return this;
    }

    /**
     * Traverse the plan by following any connected operators.
     *
     * @param operator the start point of the traversal
     * @return this instance
     */
    public PlanTraversal traverse(Operator operator) {
        return traverse(operator, null, null);
    }

    private PlanTraversal traverse(Operator operator, InputSlot<?> fromInputSlot, OutputSlot<?> fromOutputSlot) {
        if (visitedOperators.add(operator)) {
            if (this.isFollowInputs) followInputs(operator);
            if (this.isFollowOutputs) followOutputs(operator);

            if (this.traversalCallback != null) {
                this.traversalCallback.traverse(operator, fromInputSlot, fromOutputSlot);
            }
        }

        return this;
    }

    /**
     * Override to control the traversal behavior.
     */
    protected void followInputs(Operator operator) {
        Arrays.stream(operator.getAllInputs())
                .map(InputSlot::getOccupant)
                .filter(outputSlot -> outputSlot != null)
                .forEach(outputSlot -> traverse(outputSlot.getOwner(), null, outputSlot));
    }

    /**
     * Override to control the traversal behavior.
     */
    protected void followOutputs(Operator operator) {
        Arrays.stream(operator.getAllOutputs())
                .map(outputSlot -> ((OutputSlot<Object>) outputSlot).getOccupiedSlots())
                .flatMap(Collection::stream)
                .filter(inputSlot -> inputSlot != null)
                .forEach(inputSlot -> traverse(inputSlot.getOwner(), inputSlot, null));
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

    /**
     * A callback can be invoked during a plan traversal on each traversed node.
     */
    @FunctionalInterface
    public interface Callback {

        /**
         * Perform some action on a traversed operator.
         * @param operator the operator that is being traversed
         * @param fromInputSlot if the operator is being traversed via an input slot, this parameter is that slot, otherwise {@code null}
         * @param fromOutputSlot if the operator is being traversed via an output slot, this parameter is that slot, otherwise {@code null}
         */
        void traverse(Operator operator, InputSlot<?> fromInputSlot, OutputSlot<?> fromOutputSlot);

    }

}
