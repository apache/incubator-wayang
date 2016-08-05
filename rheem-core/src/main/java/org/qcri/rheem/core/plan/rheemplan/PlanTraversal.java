package org.qcri.rheem.core.plan.rheemplan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger logger = LoggerFactory.getLogger(PlanTraversal.class);

    public Set<Operator> visitedRelevantOperators = new HashSet<>(), visitedIrrelevantOperators = new HashSet<>();

    private final boolean isFollowInputs;

    private final boolean isFollowOutputs;

    /**
     * Whether to visit the {@link Operator}s contained in {@link CompositeOperator}s.
     */
    private Predicate<OperatorContainer> containerEnterCondition = container -> false;

    /**
     * If a {@link CompositeOperator} has been entered as permitted by {@link #containerEnterCondition}, then this
     * field tells whether {@link CompositeOperator}s should still be treated like {@link ElementaryOperator}s,
     * e.g., w.rt. the {@link #traversalCallback}.
     */
    private Predicate<CompositeOperator> compositeRelevanceCondition = compositeOperator -> false;

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
     * Criterion to control when to visit the {@link Operator}s contained in {@link CompositeOperator}s.
     */
    public PlanTraversal enteringContainersIf(Predicate<OperatorContainer> containerEnterCondition) {
        this.containerEnterCondition = containerEnterCondition;
        return this;
    }

    /**
     * Criterion when to consider an entered {@link CompositeOperator} as normal {@link Operator} as well.
     */
    public PlanTraversal consideringEnteredOperatorsIf(Predicate<CompositeOperator> compositeRelevanceCondition) {
        this.compositeRelevanceCondition = compositeRelevanceCondition;
        return this;
    }

    /**
     * If this method is invoked, this instance will not treat {@link Subplan}s and {@link OperatorAlternative}s as
     * normal traversed {@link Operator}s but will rather enter them and traverse their contained {@link Operator}s.
     *
     * @return this instance
     */
    public PlanTraversal traversingHierarchically() {
        return this.enteringContainersIf(operatorContainer -> true).consideringEnteredOperatorsIf(op -> false);
    }

    /**
     * If this method is invoked, this instance will not enter {@link CompositeOperator}s and instead treat
     * them just the same as {@link ElementaryOperator}s. This is the default.
     *
     * @return this instance
     */
    public PlanTraversal traversingFlat() {
        return this.enteringContainersIf(operatorContainer -> false);
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

    /**
     * Traverses a plan in a focused manner.
     *
     * @param operator     from that the traversal should be started (will not be traversed itself)
     * @param focusedSlots {@link OutputSlot}s of the {@code operator} that should be followed
     * @return this instance
     */
    public PlanTraversal traverseFocused(Operator operator, Collection<OutputSlot<?>> focusedSlots) {
        this.visitedRelevantOperators.add(operator);
        assert focusedSlots.stream().allMatch(slot -> slot.getOwner() == operator);
        this.followOutputs(focusedSlots.stream());
        return this;
    }

    /**
     * Traverses a plan.
     *
     * @param operator       from that the traversal should be started
     * @param fromInputSlot  {@link InputSlot} of the {@code operator} that has been followed
     * @param fromOutputSlot {@link OutputSlot} of the {@code operator} that has been followed
     * @return this instance
     */
    public PlanTraversal traverse(Operator operator, InputSlot<?> fromInputSlot, OutputSlot<?> fromOutputSlot) {
        // Visit the operator.
        boolean isUnseenOperator = this.visit(operator, fromInputSlot, fromOutputSlot);

        // If it has not been seen yet, continue the traversal.
        if (isUnseenOperator) {
            if (this.isFollowInputs) this.followInputs(operator);
            if (this.isFollowOutputs) this.followOutputs(operator);
        }

        return this;
    }

    /**
     * Visit the given {@link Operator}: Route to contained {@link Operator}s if possible; note the existence of
     * the {@link Operator} and whether it has been seen already; do the callback if requested.
     *
     * @param operator       from that the traversal should be started
     * @param fromInputSlot  {@link InputSlot} of the {@code operator} that has been followed
     * @param fromOutputSlot {@link OutputSlot} of the {@code operator} that has been followed
     * @return whether the {@code operator} has not been seen yet
     */
    private boolean visit(Operator operator, InputSlot<?> fromInputSlot, OutputSlot<?> fromOutputSlot) {
        // Try to do a hierarchical traversal.
        // Important: Don't add hierarchical Operators to the #visitedRelevantOperators, otherwise we might not traverse
        // them completely (e.g., by not entering via all InputSlots).
        boolean isOperatorEntered = false;
        if (!operator.isElementary()) {
            for (OperatorContainer operatorContainer : ((CompositeOperator) operator).getContainers()) {
                if (this.containerEnterCondition.test(operatorContainer)) {
                    this.enter(operatorContainer, fromInputSlot, fromOutputSlot);
                    isOperatorEntered = true;
                }
            }
        }
        boolean isRelevantOperator = !isOperatorEntered || this.compositeRelevanceCondition.test((CompositeOperator) operator);
        final Set<Operator> visitedOperators = isRelevantOperator ? this.visitedRelevantOperators : this.visitedIrrelevantOperators;
        final boolean isUnseenOperator = visitedOperators.add(operator);
        if (isUnseenOperator && isRelevantOperator && this.traversalCallback != null) {
            this.traversalCallback.traverse(operator, fromInputSlot, fromOutputSlot);
        }
        return isUnseenOperator;
    }

    /**
     * Try to enter the given {@link Operator} either via an {@link InputSlot} or {@link OutputSlot} or via its
     * sink or source.
     *
     * @param operator   a possibly non-elementary (composite) {@link Operator} to enter
     * @param fromInput  {@link InputSlot} via that the {@link Operator} should be entered
     * @param fromOutput {@link OutputSlot} via that the {@link Operator} should be entered
     * @return whether the {@link Operator} could be entered
     */
    private boolean traverseHierarchical(Operator operator, InputSlot<?> fromInput, OutputSlot<?> fromOutput) {
        if (operator.isSubplan()) {
            this.enter((Subplan) operator, fromInput, fromOutput);
        } else if (operator.isAlternative()) {
            OperatorAlternative operatorAlternative = (OperatorAlternative) operator;
            for (OperatorAlternative.Alternative alternative : operatorAlternative.getAlternatives()) {
                this.enter(alternative, fromInput, fromOutput);
            }
            return true;
        }

        assert operator.isElementary() : String.format("Unknown composite operator: %s", operator);
        return false;
    }

    /**
     * Try to enter the given {@link OperatorContainer} either via an {@link InputSlot} or {@link OutputSlot} or via its
     * sink or source.
     *
     * @param container  the {@link OperatorContainer}
     * @param fromInput  {@link InputSlot} via that the {@link Operator} should be entered
     * @param fromOutput {@link OutputSlot} via that the {@link Operator} should be entered
     * @return whether the {@link Operator} could be entered
     */
    private void enter(OperatorContainer container, InputSlot<?> fromInput, OutputSlot<?> fromOutput) {
        if (fromInput != null) {
            final Collection<InputSlot<Object>> innerInputs = container.followInput(fromInput.unchecked());
            for (InputSlot<Object> innerInput : innerInputs) {
                this.traverse(innerInput.getOwner(), innerInput, null);
            }
        } else if (fromOutput != null) {
            final OutputSlot<Object> innerOutput = container.traceOutput(fromOutput.unchecked());
            this.traverse(innerOutput.getOwner(), null, innerOutput);
        } else if (container.isSink()) {
            final Operator innerSink = container.getSink();
            this.traverse(innerSink, null, null);
        } else if (container.isSource()) {
            final Operator innerSource = container.getSource();
            this.traverse(innerSource, null, null);
        } else {
            logger.warn("Could not enter {} during hierarchical traversal.", container);
        }
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
        return this.visitedRelevantOperators.stream().filter(operatorPredicate).collect(Collectors.toList());
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
         *
         * @param operator       the operator that is being traversed
         * @param fromInputSlot  if the operator is being traversed via an input slot, this parameter is that slot, otherwise {@code null}
         * @param fromOutputSlot if the operator is being traversed via an output slot, this parameter is that slot, otherwise {@code null}
         */
        void traverse(Operator operator, InputSlot<?> fromInputSlot, OutputSlot<?> fromOutputSlot);

        /**
         * Does nothing.
         */
        Callback NOP = (operator, fromInputSlot, fromOutputSlot) -> {
        };
    }

    /**
     * A callback can be invoked during a plan traversal on each traversed node.
     */
    @FunctionalInterface
    public interface SimpleCallback {

        /**
         * Perform some action on a traversed operator.
         *
         * @param operator the operator that is being traversed
         */
        void traverse(Operator operator);
    }

}
