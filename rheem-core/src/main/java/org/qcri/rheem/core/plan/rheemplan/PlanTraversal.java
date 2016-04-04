package org.qcri.rheem.core.plan.rheemplan;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Traverse a plan. In each instance, every operator will be traversed only once.
 */
public class PlanTraversal {

    public Set<Operator> visitedOperators = new HashSet<>();

    private final boolean isFollowInputs, isFollowOutputs;

    private Callback traversalCallback = null;

    private Predicate<OutputSlot<?>> outputFollowPredicate = outputSlot -> true;

    private Predicate<InputSlot<?>> inputFollowPredicate = inputSlot -> true;

    private Predicate<InputSlot<?>> inputFollowDownstreamPredicate = inputSlot -> true;

    @Deprecated
    public PlanTraversal(boolean isFollowInputs, boolean isFollowOutputs) {
        this.isFollowInputs = isFollowInputs;
        this.isFollowOutputs = isFollowOutputs;
    }

    /**
     * @return new instance that follows any reachable {@link Operator}
     */
    public static PlanTraversal fanOut() {
        return new PlanTraversal(true, true);
    }

    /**
     * @return new instance traverses downstream only (i.e., follows {@link OutputSlot}s only)
     */
    public static PlanTraversal downstream() {
        return new PlanTraversal(false, true);
    }

    /**
     * @return new instance traverses downstream only (i.e., follows {@link OutputSlot}s only)
     */
    public static PlanTraversal upstream() {
        return new PlanTraversal(true, false);
    }

    public PlanTraversal withCallback(Callback traversalCallback) {
        this.traversalCallback = traversalCallback;
        return this;
    }

    public PlanTraversal withCallback(SimpleCallback traversalCallback) {
        this.traversalCallback = (operator, fromInputSlot, fromOutputSlot) -> traversalCallback.traverse(operator);
        return this;
    }

    public PlanTraversal followingInputsIf(Predicate<InputSlot<?>> inputFollowPredicate) {
        this.inputFollowPredicate = inputFollowPredicate;
        return this;
    }

    public PlanTraversal followingInputsDownstreamIf(Predicate<InputSlot<?>> inputFollowPredicate) {
        this.inputFollowDownstreamPredicate = inputFollowPredicate;
        return this;
    }

    public PlanTraversal followingOutputsIf(Predicate<OutputSlot<?>> outputFollowPredicate) {
        this.outputFollowPredicate = outputFollowPredicate;
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
     * Traversing as with {@link #traverse(Operator, InputSlot, OutputSlot)} for every operator.
     */
    public PlanTraversal traverse(Stream<? extends Operator> operators) {
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
        return this.traverse(operator, null, null);
    }

    public PlanTraversal traverseFocused(Operator operator, Collection<OutputSlot<?>> focusedSlots) {
        this.visitedOperators.add(operator);
        assert focusedSlots.stream().allMatch(slot -> slot.getOwner() == operator);
        this.followOutputs(focusedSlots.stream());
        return this;
    }

    public PlanTraversal traverse(Operator operator, InputSlot<?> fromInputSlot, OutputSlot<?> fromOutputSlot) {
        if (this.visitedOperators.add(operator)) {
            if (this.traversalCallback != null) {
                this.traversalCallback.traverse(operator, fromInputSlot, fromOutputSlot);
            }

            if (this.isFollowInputs) this.followInputs(operator);
            if (this.isFollowOutputs) this.followOutputs(operator);
        }

        return this;
    }

    /**
     * Override to control the traversal behavior.
     */
    protected void followInputs(Operator operator) {
        Arrays.stream(operator.getAllInputs())
                .filter(this.inputFollowPredicate)
                .map(InputSlot::getOccupant)
                .filter(outputSlot -> outputSlot != null)
                .forEach(outputSlot -> this.traverse(outputSlot.getOwner(), null, outputSlot));
    }

    /**
     * Call {@link #followOutputs(Stream)} for all the {@link OutputSlot}s of the given {@code operator}.
     */
    private void followOutputs(Operator operator) {
        this.followOutputs(Arrays.stream(operator.getAllOutputs()));
    }
    /**
     * Override to control the traversal behavior.
     */
    protected void followOutputs(Stream<OutputSlot<?>> outputSlots) {
        outputSlots
                .filter(this.outputFollowPredicate)
                .map(outputSlot -> ((OutputSlot<Object>) outputSlot).getOccupiedSlots())
                .flatMap(Collection::stream)
                .filter(inputSlot -> inputSlot != null)
                .filter(this.inputFollowDownstreamPredicate)
                .forEach(inputSlot -> this.traverse(inputSlot.getOwner(), inputSlot, null));
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
     * Retrieve all traversed operators.
     *
     * @return previously traversed operators
     */
    public Collection<Operator> getTraversedNodes() {
        return this.getTraversedNodesWith(operator -> true);
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

        /**
         * Does nothing.
         */
        Callback NOP = (operator, fromInputSlot, fromOutputSlot) -> {};
    }

    /**
     * A callback can be invoked during a plan traversal on each traversed node.
     */
    @FunctionalInterface
    public interface SimpleCallback {

        /**
         * Perform some action on a traversed operator.
         * @param operator the operator that is being traversed
         */
        void traverse(Operator operator);
    }

}
