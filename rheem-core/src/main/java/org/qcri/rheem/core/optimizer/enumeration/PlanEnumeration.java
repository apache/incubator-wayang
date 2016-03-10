package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.platform.Junction;
import org.qcri.rheem.core.util.MultiMap;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.Tuple;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Represents a collection of {@link PartialPlan}s that all implement the same section of a {@link RheemPlan} (which
 * is assumed to contain {@link OperatorAlternative}s in general).
 * <p>
 * <p><i>TODO: Describe algebra.</i>
 * The outputs are mapped to this {@link RheemPlan} if the plans are partial.</p>
 */
public class PlanEnumeration {

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
     * Concatenates the {@code baseEnumeration} via its {@code openOutputSlot} to the {@code targetEnumerations}.
     * All {@link PlanEnumeration}s should be distinct.
     */
    public PlanEnumeration concatenate(OutputSlot<?> openOutputSlot,
                                       Map<InputSlot<?>, PlanEnumeration> targetEnumerations,
                                       OptimizationContext optimizationContext) {

        assert this.getServingOutputSlots().stream()
                .map(Tuple::getField0)
                .anyMatch(openOutputSlot::equals);
        assert !targetEnumerations.isEmpty();


        PlanEnumeration result = new PlanEnumeration();
        result.scope.addAll(this.getScope());
        result.requestedInputSlots.addAll(this.getRequestedInputSlots());
        result.servingOutputSlots.addAll(this.getServingOutputSlots());
        result.executedTasks.putAll(this.getExecutedTasks());


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

        result.partialPlans.addAll(
                this.concatenatePartialPlans(openOutputSlot, targetEnumerations, optimizationContext, result)
        );

        // Build the instance.
        return result;
    }

    /**
     * Concatenates all {@link PartialPlan}s of the {@code baseEnumeration} via its {@code openOutputSlot}
     * to the {@code targetEnumerations}' {@link PartialPlan}s.
     * All {@link PlanEnumeration}s should be distinct.
     */
    public Collection<PartialPlan> concatenatePartialPlans(OutputSlot<?> openOutputSlot,
                                                           Map<InputSlot<?>, PlanEnumeration> targetEnumerations,
                                                           OptimizationContext optimizationContext,
                                                           PlanEnumeration concatenationEnumeration) {

        Collection<PartialPlan> concatenations = new LinkedList<>();

        // Sort the PlanEnumerations by their respective open InputSlot or OutputSlot.
        final MultiMap<OutputSlot<?>, PartialPlan> basePlanGroups = new MultiMap<>();
        for (PartialPlan basePlan : this.getPartialPlans()) {
            final OutputSlot<?> openOutput = basePlan.findExecutionOperatorOutput(openOutputSlot);
            assert openOutput != null;
            basePlanGroups.putSingle(openOutput, basePlan);
        }
        List<MultiMap<InputSlot<?>, PartialPlan>> targetPlanGroupList = new ArrayList<>(targetEnumerations.size());
        for (Map.Entry<InputSlot<?>, PlanEnumeration> entry : targetEnumerations.entrySet()) {
            final InputSlot<?> openInputSlot = entry.getKey();
            final PlanEnumeration targetEnumeration = entry.getValue();
            MultiMap<InputSlot<?>, PartialPlan> targetPlanGroups = new MultiMap<>();
            for (PartialPlan targetPlan : targetEnumeration.getPartialPlans()) {
                // TODO: In general, we might face multiple mapped InputSlots, although this is presumably a rare case.
                final InputSlot<?> openInput = RheemCollections.getSingle(
                        targetPlan.findExecutionOperatorInputs(openInputSlot));
                targetPlanGroups.putSingle(openInput, targetPlan);
            }
            targetPlanGroupList.add(targetPlanGroups);
        }

        // Prepare the cross product of all InputSlots.
        List<Set<Map.Entry<InputSlot<?>, Set<PartialPlan>>>> targetPlanGroupEntrySet =
                RheemCollections.map(targetPlanGroupList, MultiMap::entrySet);
        final Iterable<List<Map.Entry<InputSlot<?>, Set<PartialPlan>>>> targetPlanGroupCrossProduct =
                RheemCollections.streamedCrossProduct(targetPlanGroupEntrySet);

        // Iterate all InputSlot/OutputSlot combinations.
        for (List<Map.Entry<InputSlot<?>, Set<PartialPlan>>> targetPlanGroupEntries : targetPlanGroupCrossProduct) {
            List<InputSlot<?>> inputs = RheemCollections.map(targetPlanGroupEntries, Map.Entry::getKey);
            for (Map.Entry<OutputSlot<?>, Set<PartialPlan>> basePlanGroupEntry : basePlanGroups.entrySet()) {
                final OutputSlot<?> output = basePlanGroupEntry.getKey();
                final Operator outputOperator = output.getOwner();
                assert outputOperator.isExecutionOperator();
                final Junction junction = Junction.create(output, inputs, optimizationContext);
                if (junction == null) continue;

                // If we found a junction, then we can enumerator all PartialPlan combinations
                final List<Set<PartialPlan>> targetPlans = RheemCollections.map(targetPlanGroupEntries, Map.Entry::getValue);
                for (List<PartialPlan> targetPlanList : RheemCollections.streamedCrossProduct(targetPlans)) {
                    for (PartialPlan basePlan : basePlanGroupEntry.getValue()) {
                        PartialPlan concatenatedPlan = basePlan.concatenate(targetPlanList, junction, concatenationEnumeration);
                        if (concatenatedPlan != null) {
                            concatenations.add(concatenatedPlan);
                        }
                    }
                }
            }
        }

        return concatenations;
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
