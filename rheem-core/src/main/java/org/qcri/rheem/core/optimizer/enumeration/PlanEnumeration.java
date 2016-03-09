package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents a collection of {@link PartialPlan}s that all implement the same section of a {@link RheemPlan} (which
 * is assumed to contain {@link OperatorAlternative}s in general).
 * <p>
 * <p><i>TODO: Describe algebra.</i>
 * The outputs are mapped to this {@link RheemPlan} if the plans are partial.</p>
 */
public class PlanEnumeration {

    private static final Logger LOGGER = LoggerFactory.getLogger(PlanEnumeration.class);

    /**
     * The {@link OperatorAlternative}s for that an {@link OperatorAlternative.Alternative} has been picked.
     */
    final Set<OperatorAlternative> scope;

    /**
     * Outermost {@link InputSlot}s that are not satisfied in this instance.
     */
    final Set<InputSlot<?>> requestedInputSlots;

    /**
     * Combinations of {@link OutputSlot}s and {@link InputSlot}, where the former is served by this instance and the
     * latter is not yet assigned in this instance. If there is no such {@link InputSlot} (because we are enumerating
     * an {@link OperatorAlternative.Alternative}, then we put {@code null} instead of it.
     */
    final Set<Tuple<OutputSlot<?>, InputSlot<?>>> servingOutputSlots;

    /**
     * {@link PartialPlan}s contained in this instance.
     */
    final Collection<PartialPlan> partialPlans;

    /**
     *
     */
    final Map<ExecutionOperator, ExecutionTask> executedTasks;

    /**
     * Creates a new instance.
     */
    public PlanEnumeration() {
        this(new HashSet<>(), new HashSet<>(), new HashSet<>());
    }

    /**
     * Creates a new instance.
     */
    private PlanEnumeration(Set<OperatorAlternative> scope,
                            Set<InputSlot<?>> requestedInputSlots,
                            Set<Tuple<OutputSlot<?>, InputSlot<?>>> servingOutputSlots) {
        this(scope, requestedInputSlots, servingOutputSlots, new LinkedList<>(), new HashMap<>());
    }

    /**
     * Creates a new instance.
     */
    private PlanEnumeration(Set<OperatorAlternative> scope,
                            Set<InputSlot<?>> requestedInputSlots,
                            Set<Tuple<OutputSlot<?>, InputSlot<?>>> servingOutputSlots,
                            Collection<PartialPlan> partialPlans,
                            Map<ExecutionOperator, ExecutionTask> executedTasks) {
        this.scope = scope;
        this.requestedInputSlots = requestedInputSlots;
        this.servingOutputSlots = servingOutputSlots;
        this.partialPlans = partialPlans;
        this.executedTasks = executedTasks;
    }

    /**
     * Create an instance for a single {@link ExecutionOperator}.
     *
     * @param operator the mentioned {@link ExecutionOperator}
     * @return the new instance
     */
    static PlanEnumeration createSingleton(ExecutionOperator operator, OptimizationContext optimizationContext) {
        final PlanEnumeration enumeration = createFor(operator, operator);
        final PartialPlan singletonPartialPlan = enumeration.createSingletonPartialPlan(operator, optimizationContext);
        enumeration.add(singletonPartialPlan);
        return enumeration;
    }

    /**
     * Creates a new instance.
     *
     * @param inputOperator  provides the requested {@link InputSlot}s
     * @param outputOperator provides the requested {@link OutputSlot}s
     * @return the new instance
     */
    static PlanEnumeration createFor(Operator inputOperator, Operator outputOperator) {
        return createFor(inputOperator, input -> true, outputOperator, output -> true);
    }

    /**
     * Creates a new instance.
     *
     * @param inputOperator       provides the requested {@link InputSlot}s
     * @param inputSlotPredicate  can narrow down the {@link InputSlot}s
     * @param outputOperator      provides the requested {@link OutputSlot}s
     * @param outputSlotPredicate can narrow down the {@link OutputSlot}s
     * @return the new instance
     */
    static PlanEnumeration createFor(Operator inputOperator,
                                     Predicate<InputSlot<?>> inputSlotPredicate,
                                     Operator outputOperator,
                                     Predicate<OutputSlot<?>> outputSlotPredicate) {

        final PlanEnumeration instance = new PlanEnumeration();
        for (InputSlot<?> inputSlot : inputOperator.getAllInputs()) {
            if (inputSlotPredicate.test(inputSlot)) {
                instance.requestedInputSlots.add(inputSlot);
            }
        }

        for (OutputSlot outputSlot : outputOperator.getAllOutputs()) {
            if (outputSlotPredicate.test(outputSlot)) {
                List<InputSlot> inputSlots = outputSlot.getOccupiedSlots();
                if (inputSlots.isEmpty()) {
                    inputSlots = Collections.singletonList(null); // InputSlot is probably in a surrounding plan.
                }
                for (InputSlot inputSlot : inputSlots) {
                    instance.servingOutputSlots.add(new Tuple<>(outputSlot, inputSlot));
                }
            }
        }

        return instance;
    }

    private static void assertMatchingInterface(PlanEnumeration instance1, PlanEnumeration instance2) {
        if (!instance1.requestedInputSlots.equals(instance2.requestedInputSlots)) {
            throw new IllegalArgumentException("Input slots are not matching.");
        }

        if (!instance1.servingOutputSlots.equals(instance2.servingOutputSlots)) {
            throw new IllegalArgumentException("Output slots are not matching.");
        }

    }

    /**
     * Join two instances. This potentially increases number of {@link #partialPlans} to the product
     * of the numbers of partial plans of the two instances.
     * <p>The join is guided by two interaction points of the two instances.
     * <ol>
     * <li><b>Common scope.</b> If the two instances have a common scope (they both picked assignments for the some
     * {@link OperatorAlternative}(s)), then only those pairs of {@link PartialPlan}s can be merged, that agree
     * on this assignment.</li>
     * <li><b>Concatenation.</b> If one instance provides an {@link OutputSlot} that feeds the {@link InputSlot} of
     * the other instance (concatenation point), then pairs of {@link PartialPlan}s to be merged must be compatible
     * in this concatenation point.</li>
     * </ol>
     * Without these interaction points, the join degrades basically to a Cartesian product. This is an unexpected
     * situation.</p>
     *
     * @param that instance to join
     * @return a new instance representing the join product
     */
    public PlanEnumeration join(PlanEnumeration that) {
        // TODO: How can we guarantee that the two instances are not incompatible (because their scopes imply
        // different choices for some OperatorAlternatives)

        // Figure out if the two instance can be concatenated.
        // To this end, there muse be some remaining OutputSlot of this instance that connects to an InputSlot
        // of the other instance. At first, we collect these touchpoints.
        final Set<InputSlot<?>> concatenatableInputs = this.collectConcatenatableInputs(that);
        if (concatenatableInputs.isEmpty()) {
            LOGGER.warn("Could not find a (directed) point of touch when joining to instances.");
        }

        //-----------------------------\\
        // Set up #requestedInputSlots \\
        //-----------------------------\\

        // Find out, which InputSlots are requested by both instances.
        final Set<InputSlot<?>> requestedInputSlots =
                new HashSet<>(this.requestedInputSlots.size() + that.requestedInputSlots.size());
        Stream.concat(this.requestedInputSlots.stream(), that.requestedInputSlots.stream())
                .filter(requestedInput -> !concatenatableInputs.contains(requestedInput))
                .forEach(requestedInputSlots::add);

        //----------------------------\\
        // Set up #servingOutputSlots \\
        //----------------------------\\

        // Find out, which InputSlots are served by both instances.
        final Set<Tuple<OutputSlot<?>, InputSlot<?>>> servingOutputSlots =
                new HashSet<>(this.servingOutputSlots.size() + that.servingOutputSlots.size());
        Stream.concat(this.servingOutputSlots.stream(), that.servingOutputSlots.stream())
                .filter(serving -> !concatenatableInputs.contains(serving.getField1()))
                .forEach(servingOutputSlots::add);


        //---------------\\
        // Set up #scope \\
        //---------------\\

        // It's simply the union of both scopes.
        final Set<OperatorAlternative> newScope = new HashSet<>(this.scope.size() + that.scope.size());
        newScope.addAll(this.scope);
        newScope.addAll(that.scope);

        //---------------------------\\
        // Set up #terminalOperators \\
        //---------------------------\\
        // NOP: We ignore them so far.

        //-----------------------//
        // Set up #executedTasks \\
        //-----------------------//
        final Map<ExecutionOperator, ExecutionTask> executedTasks = new HashMap<>(this.executedTasks);
        executedTasks.putAll(that.executedTasks);

        //----------------------\\
        // Set up #partialPlans \\
        //----------------------\\
        PlanEnumeration planEnumeration =
                new PlanEnumeration(newScope, requestedInputSlots, servingOutputSlots, new LinkedList<>(), executedTasks);
        this.joinPartialPlansUsingNestedLoops(that, planEnumeration);

        // Build the instance.
        return planEnumeration;
    }

    /**
     * Concatenates the {@code baseEnumeration} via its {@code openOutputSlot} to the {@code targetEnumerations}.
     * All {@link PlanEnumeration}s should be distinct.
     */
    public static PlanEnumeration concatenate(PlanEnumeration baseEnumeration,
                                              OutputSlot<?> openOutputSlot,
                                              Map<InputSlot<?>, PlanEnumeration> targetEnumerations,
                                              OptimizationContext optimizationContext) {

        assert baseEnumeration.getServingOutputSlots().stream()
                .map(Tuple::getField0)
                .anyMatch(openOutputSlot::equals);
        assert !targetEnumerations.isEmpty();


        PlanEnumeration result = new PlanEnumeration();
        result.scope.addAll(baseEnumeration.getScope());
        result.requestedInputSlots.addAll(baseEnumeration.getRequestedInputSlots());
        result.servingOutputSlots.addAll(baseEnumeration.getServingOutputSlots());
        result.executedTasks.putAll(baseEnumeration.getExecutedTasks());


        for (Map.Entry<InputSlot<?>, PlanEnumeration> entry : targetEnumerations.entrySet()) {
            final InputSlot<?> openInputSlot = entry.getKey();
            final PlanEnumeration targetEnumeration = entry.getValue();
            result.scope.addAll(targetEnumeration.getScope());
            result.requestedInputSlots.addAll(targetEnumeration.getRequestedInputSlots());
            result.servingOutputSlots.addAll(targetEnumeration.getServingOutputSlots());
            result.executedTasks.putAll(targetEnumeration.getExecutedTasks());
        }

        // NB: We need to store remove the InputSlots only here, because a single targetEnumeration
        // might service multiple InputSlots. If this targetEnumeration is then also the baseEnumeration, it might
        // re-request already serviced InputSlots, although already deleted.
        result.requestedInputSlots.removeAll(targetEnumerations.keySet());
        result.servingOutputSlots.removeIf(slotService -> slotService.getField0().equals(openOutputSlot));

        result.partialPlans.addAll(PartialPlan.concatenate(baseEnumeration,
                openOutputSlot,
                targetEnumerations,
                optimizationContext,
                result));

        // Build the instance.
        return result;
    }


    /**
     * Collect all {@link InputSlot}s that are served by one instance and requested by the other.
     */
    private Set<InputSlot<?>> collectConcatenatableInputs(PlanEnumeration that) {
        final Set<InputSlot<?>> connectedInputSlots = new HashSet<>(2);
        this.collectConcatenableInputsDownstream(that, connectedInputSlots);
        that.collectConcatenableInputsDownstream(this, connectedInputSlots);
        return connectedInputSlots;
    }

    /**
     * Collect all {@link InputSlot}s that are served by this instance and requested by {@code that} instance.
     */
    private void collectConcatenableInputsDownstream(PlanEnumeration that, Set<InputSlot<?>> collector) {
        assert that != null;
        this.servingOutputSlots.stream()
                .map(Tuple::getField1)
                .filter(Objects::nonNull)
                .filter(that.requestedInputSlots::contains)
                .forEach(collector::add);
    }

    /**
     * Collect all {@link InputSlot}s that are served by this instance and requested by {@code that} instance.
     */
    private Set<InputSlot<?>> collectConcatenableInputsDownstream(PlanEnumeration that) {
        Set<InputSlot<?>> inputSlots = new HashSet<>(2);
        this.collectConcatenableInputsDownstream(that, inputSlots);
        return inputSlots;
    }

    private List<OperatorAlternative> intersectScopeWith(PlanEnumeration that) {
        return this.scope.stream().filter(that.scope::contains).collect(Collectors.toList());
    }

    /**
     * Add a {@link PartialPlan} to this instance.
     *
     * @param partialPlan to be added
     */
    public void add(PartialPlan partialPlan) {
        // TODO: Check if the plan conforms to this instance.
        this.partialPlans.add(partialPlan);
        assert partialPlan.getTimeEstimate() != null;
        partialPlan.setPlanEnumeration(this);
    }

    /**
     * Joins the {@link #partialPlans} of this instance with those of {@code that} using nested loops.
     */
    private void joinPartialPlansUsingNestedLoops(PlanEnumeration that, PlanEnumeration target) {
        final List<OperatorAlternative> commonScope = this.intersectScopeWith(that);
        Set<InputSlot<?>> thisToThatConcatenatableInputs = this.collectConcatenableInputsDownstream(that);
        Set<InputSlot<?>> thatToThisConcatenatableInputs = that.collectConcatenableInputsDownstream(this);

        for (PartialPlan plan1 : this.partialPlans) {
            for (PartialPlan plan2 : that.partialPlans) {
                final PartialPlan newPartialPlan = plan1.join(
                        plan2,
                        commonScope,
                        thisToThatConcatenatableInputs,
                        thatToThisConcatenatableInputs,
                        target);
                if (newPartialPlan != null) {
                    target.add(newPartialPlan);
                }
            }
        }
    }

    /**
     * Creates a new instance for exactly one {@link ExecutionOperator}.
     *
     * @param executionOperator   will be wrapped in the new instance
     * @param optimizationContext
     * @return the new instance
     */
    private PartialPlan createSingletonPartialPlan(ExecutionOperator executionOperator, OptimizationContext optimizationContext) {
        final PartialPlan partialPlan = new PartialPlan(this, new HashMap<>(0), Collections.singletonList(executionOperator));
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.getOperatorContext(executionOperator);
        partialPlan.addToTimeEstimate(operatorContext.getTimeEstimate());
        return partialPlan;
    }

    /**
     * Unions the {@link PartialPlan}s of this and {@code that} instance. The operation is in-place, i.e., this instance
     * is modified to form the result.
     *
     * @param that the instance to compute the union with
     */
    public void unionInPlace(PlanEnumeration that) {
        assertMatchingInterface(this, that);
        this.scope.addAll(that.scope);
        that.partialPlans.forEach(partialPlan -> {
            this.partialPlans.add(partialPlan);
            partialPlan.setPlanEnumeration(this);
        });
        that.partialPlans.clear();
    }

    /**
     * Create a new instance that equals this instance but redirects via
     * {@link OperatorAlternative.Alternative#getSlotMapping()}.
     *
     * @param alternative the alternative to escape or {@code null} if none (in that case, this method returns the
     *                    this instance)
     */
    public PlanEnumeration escape(OperatorAlternative.Alternative alternative) {
        if (alternative == null) return this;
        PlanEnumeration escapedInstance = new PlanEnumeration();
        final OperatorAlternative operatorAlternative = alternative.getOperatorAlternative();

        // Copy and widen the scope.
        escapedInstance.scope.addAll(this.scope);
        escapedInstance.scope.add(operatorAlternative);

        // Escape the input slots.
        for (InputSlot inputSlot : this.requestedInputSlots) {
            final InputSlot escapedInput = alternative.getSlotMapping().resolveUpstream(inputSlot);
            if (escapedInput != null) {
                escapedInstance.requestedInputSlots.add(escapedInput);
            }
        }

        // Escape the output slots.
        for (Tuple<OutputSlot<?>, InputSlot<?>> link : this.servingOutputSlots) {
            if (link.field1 != null) {
                throw new IllegalStateException("Cannot escape a connected output slot.");
            }
            final Collection<OutputSlot<Object>> resolvedOutputSlots =
                    alternative.getSlotMapping().resolveDownstream(link.field0.unchecked());
            for (OutputSlot escapedOutput : resolvedOutputSlots) {
                final List<InputSlot<?>> occupiedInputs = escapedOutput.getOccupiedSlots();
                if (occupiedInputs.isEmpty()) {
                    escapedInstance.servingOutputSlots.add(new Tuple<>(escapedOutput, null));
                } else {
                    for (InputSlot inputSlot : occupiedInputs) {
                        escapedInstance.servingOutputSlots.add(new Tuple<>(escapedOutput, inputSlot));
                    }
                }
            }
        }


        // Escape the PartialPlan instances.
        for (PartialPlan partialPlan : this.partialPlans) {
            escapedInstance.partialPlans.add(partialPlan.escape(alternative, escapedInstance));
        }

        return escapedInstance;
    }

    public Collection<PartialPlan> getPartialPlans() {
        return this.partialPlans;
    }

    public Set<InputSlot<?>> getRequestedInputSlots() {
        return this.requestedInputSlots;
    }

    public Set<Tuple<OutputSlot<?>, InputSlot<?>>> getServingOutputSlots() {
        return this.servingOutputSlots;
    }

    public Set<OperatorAlternative> getScope() {
        return this.scope;
    }

    public Map<ExecutionOperator, ExecutionTask> getExecutedTasks() {
        return this.executedTasks;
    }

    @Override
    public String toString() {
        return String.format("%s[%dx, inputs=%s, outputs=%s]", this.getClass().getSimpleName(),
                this.getPartialPlans().size(),
                this.requestedInputSlots, this.servingOutputSlots.stream()
                        .map(Tuple::getField0)
                        .distinct()
                        .collect(Collectors.toList())
        );
    }
}
