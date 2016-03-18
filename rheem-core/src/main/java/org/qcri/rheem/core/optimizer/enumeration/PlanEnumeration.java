package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.util.MultiMap;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.Tuple;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Represents a collection of {@link PlanImplementation}s that all implement the same section of a {@link RheemPlan} (which
 * is assumed to contain {@link OperatorAlternative}s in general).
 * <p>Instances can be mutated and combined in algebraic manner. In particular, instances can be unioned if they implement
 * the same part of the {@link RheemPlan}, concatenated if there are contact points, and pruned.</p>
 */
public class PlanEnumeration {

    /**
     * The {@link OperatorAlternative}s for that an {@link OperatorAlternative.Alternative} has been picked.
     */
    final Set<OperatorAlternative> scope;

    /**
     * {@link InputSlot}s that are not satisfied in this instance.
     */
    final Set<InputSlot<?>> requestedInputSlots;

    /**
     * Combinations of {@link OutputSlot}s and {@link InputSlot}, where the former is served by this instance and the
     * latter is not yet assigned in this instance. If there is no such {@link InputSlot} (because we are enumerating
     * an {@link OperatorAlternative.Alternative}, then we put {@code null} instead of it.
     */
    final Set<Tuple<OutputSlot<?>, InputSlot<?>>> servingOutputSlots;

    /**
     * {@link PlanImplementation}s contained in this instance.
     */
    final Collection<PlanImplementation> planImplementations;

    /**
     * {@link ExecutionTask}s that have already been executed.
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
                            Collection<PlanImplementation> planImplementations,
                            Map<ExecutionOperator, ExecutionTask> executedTasks) {
        this.scope = scope;
        this.requestedInputSlots = requestedInputSlots;
        this.servingOutputSlots = servingOutputSlots;
        this.planImplementations = planImplementations;
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
        final PlanImplementation singletonPlanImplementation = enumeration.createSingletonPartialPlan(operator, optimizationContext);
        enumeration.add(singletonPlanImplementation);
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

    /**
     * Asserts that two given instances enumerate the same part of a {@link RheemPlan}.
     */
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
                .anyMatch(openOutputSlot::equals)
                : String.format("Cannot concatenate %s: it is not a served output.", openOutputSlot);
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

        result.planImplementations.addAll(
                this.concatenatePartialPlans(openOutputSlot, targetEnumerations, optimizationContext, result)
        );

        // Build the instance.
        return result;
    }

    /**
     * Concatenates all {@link PlanImplementation}s of the {@code baseEnumeration} via its {@code openOutputSlot}
     * to the {@code targetEnumerations}' {@link PlanImplementation}s.
     * All {@link PlanEnumeration}s should be distinct.
     */
    private Collection<PlanImplementation> concatenatePartialPlans(OutputSlot<?> openOutputSlot,
                                                                   Map<InputSlot<?>, PlanEnumeration> targetEnumerations,
                                                                   OptimizationContext optimizationContext,
                                                                   PlanEnumeration concatenationEnumeration) {

//        // Group the base and target PlanImplementations by their operator.
//        final MultiMap<Collection<OutputSlot<?>>, PlanImplementation> basePlanGroups =
//                this.groupImplementationsByOutput(openOutputSlot);
//        final List<MultiMap<Set<InputSlot<?>>, PlanImplementation>> targetPlanGroupList =
//                groupImplementationsByInput(targetEnumerations);
//
//
//        // Allocate result collector.
//        Collection<PlanImplementation> result = new LinkedList<>();
//
//        // Iterate all InputSlot/OutputSlot combinations.
//        List<Set<Map.Entry<Set<InputSlot<?>>, Set<PlanImplementation>>>> targetPlanGroupEntrySet =
//                RheemCollections.map(targetPlanGroupList, MultiMap::entrySet);
//        final Iterable<List<Map.Entry<Set<InputSlot<?>>, Set<PlanImplementation>>>> targetPlanGroupCrossProduct =
//                RheemCollections.streamedCrossProduct(targetPlanGroupEntrySet);
//        for (List<Map.Entry<Set<InputSlot<?>>, Set<PlanImplementation>>> targetPlanGroupEntries : targetPlanGroupCrossProduct) {
//            // Flatten the requested InputSlots.
//            final List<InputSlot<?>> inputs = targetPlanGroupEntries.stream()
//                    .map(Map.Entry::getKey)
//                    .flatMap(Collection::stream)
//                    .distinct()
//                    .collect(Collectors.toCollection(() -> new ArrayList<>(4)));
//
//            for (Map.Entry<Collection<OutputSlot<?>>, Set<PlanImplementation>> basePlanGroupEntry : basePlanGroups.entrySet()) {
//
//                final Collection<OutputSlot<?>> outputs = basePlanGroupEntry.getKey();
//                for (final OutputSlot<?> output : outputs) {
//
//                    // Construct a Junction between the ExecutionOperators.
//                    final Operator outputOperator = output.getOwner();
//                    assert outputOperator.isExecutionOperator()
//                            : String.format("Expected execution operator, found %s.", outputOperator);
//                    final Junction junction = Junction.create(output, inputs, optimizationContext);
//                    if (junction == null) continue;
//
//                    // If we found a junction, then we can enumerate all PlanImplementation combinations
//                    final List<Set<PlanImplementation>> targetPlans = RheemCollections.map(targetPlanGroupEntries, Map.Entry::getValue);
//                    for (List<PlanImplementation> targetPlanList : RheemCollections.streamedCrossProduct(targetPlans)) {
//                        for (PlanImplementation basePlan : basePlanGroupEntry.getValue()) {
//                            PlanImplementation concatenatedPlan = basePlan.concatenate(targetPlanList, junction, concatenationEnumeration);
//                            if (concatenatedPlan != null) {
//                                result.add(concatenatedPlan);
//                            }
//                        }
//                    }
//                }
//            }
//        }
//
//        return result;

        // Simple implementation waives optimization potential.

        // Allocated result collector.
        Collection<PlanImplementation> resultCollector = new LinkedList<>();

        // Iterate over the cross product of PlanImplementations.
        List<Map.Entry<InputSlot<?>, PlanEnumeration>> targetEnumerationEntries = new ArrayList<>(targetEnumerations.entrySet());
        List<InputSlot<?>> inputSlots = RheemCollections.map(targetEnumerationEntries, Map.Entry::getKey);
        List<Collection<PlanImplementation>> targetEnumerationImplList = RheemCollections.map(
                targetEnumerationEntries,
                entry -> entry.getValue().getPlanImplementations()
        );
        for (List<PlanImplementation> targetImpls : RheemCollections.streamedCrossProduct(targetEnumerationImplList)) {
            for (PlanImplementation thisImpl : this.getPlanImplementations()) {

                // Concatenate the PlanImplementations.
                resultCollector.add(
                        thisImpl.concatenate(openOutputSlot, targetImpls, inputSlots, concatenationEnumeration, optimizationContext)
                );
            }
        }

        return resultCollector;
    }

    /**
     * Groups all {@link #planImplementations} by their {@link ExecutionOperator} for the {@code output}. Additionally
     * preserves the very (nested) {@link PlanImplementation} in that {@code output} resides.
     */
    private MultiMap<Collection<OutputSlot<?>>, Tuple<PlanImplementation, PlanImplementation>>
    groupImplementationsByOutput(OutputSlot<?> output) {
        // Sort the PlanEnumerations by their respective open InputSlot or OutputSlot.
        final MultiMap<Collection<OutputSlot<?>>, Tuple<PlanImplementation, PlanImplementation>> basePlanGroups =
                new MultiMap<>();
        for (PlanImplementation basePlan : this.getPlanImplementations()) {
            final Collection<Tuple<OutputSlot<?>, PlanImplementation>> execOpOutputsWithContext =
                    basePlan.findExecutionOperatorOutputWithContext(output);
            assert execOpOutputsWithContext != null && !execOpOutputsWithContext.isEmpty()
                    : String.format("No outputs found for %s.", output);

//       todo     basePlanGroups.putSingle(execOpOutputsWithContext, basePlan);
        }
        return basePlanGroups;
    }

    /**
     * For each given instance, group the {@link #planImplementations} by their {@link ExecutionOperator} for the
     * associated {@link InputSlot}.
     *
     * @param enumerations a mapping from {@link InputSlot}s to {@link PlanEnumeration}s that request this input
     * @return a {@link List} with an element for each {@code enumerations} entry; each entry groups the
     * {@link PlanImplementation}s of the {@link PlanEnumeration} that share the same {@link ExecutionOperator}s for
     * the requested {@link InputSlot}
     */
    private static List<MultiMap<Set<InputSlot<?>>, PlanImplementation>> groupImplementationsByInput(
            Map<InputSlot<?>, PlanEnumeration> enumerations) {

        // Prepare a collector for the results.
        List<MultiMap<Set<InputSlot<?>>, PlanImplementation>> targetPlanGroupList = new ArrayList<>(enumerations.size());

        // Go over all PlanEnumerations.
        for (Map.Entry<InputSlot<?>, PlanEnumeration> entry : enumerations.entrySet()) {
            // Extract the requested InputSlot and the associated PlanEnumeration requesting it.
            final InputSlot<?> requestedInput = entry.getKey();
            final PlanEnumeration targetEnumeration = entry.getValue();


            MultiMap<Set<InputSlot<?>>, PlanImplementation> targetPlanGroups = new MultiMap<>();
            for (PlanImplementation planImpl : targetEnumeration.getPlanImplementations()) {
                final Collection<InputSlot<?>> openInput = planImpl.findExecutionOperatorInputs(requestedInput);
                targetPlanGroups.putSingle(RheemCollections.asSet(openInput), planImpl);
            }
            targetPlanGroupList.add(targetPlanGroups);
        }
        return targetPlanGroupList;
    }


    /**
     * Add a {@link PlanImplementation} to this instance.
     *
     * @param planImplementation to be added
     */
    public void add(PlanImplementation planImplementation) {
        // TODO: Check if the plan conforms to this instance.
        this.planImplementations.add(planImplementation);
        assert planImplementation.getTimeEstimate() != null;
        planImplementation.setPlanEnumeration(this);
    }

    /**
     * Creates a new instance for exactly one {@link ExecutionOperator}.
     *
     * @param executionOperator   will be wrapped in the new instance
     * @param optimizationContext
     * @return the new instance
     */
    private PlanImplementation createSingletonPartialPlan(ExecutionOperator executionOperator, OptimizationContext optimizationContext) {
        final PlanImplementation planImplementation = new PlanImplementation(this, new HashMap<>(0), Collections.singletonList(executionOperator));
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.getOperatorContext(executionOperator);
        planImplementation.addToTimeEstimate(operatorContext.getTimeEstimate());
        return planImplementation;
    }

    /**
     * Unions the {@link PlanImplementation}s of this and {@code that} instance. The operation is in-place, i.e., this instance
     * is modified to form the result.
     *
     * @param that the instance to compute the union with
     */
    public void unionInPlace(PlanEnumeration that) {
        assertMatchingInterface(this, that);
        this.scope.addAll(that.scope);
        that.planImplementations.forEach(partialPlan -> {
            this.planImplementations.add(partialPlan);
            partialPlan.setPlanEnumeration(this);
        });
        that.planImplementations.clear();
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


        // Escape the PlanImplementation instances.
        for (PlanImplementation planImplementation : this.planImplementations) {
            escapedInstance.planImplementations.add(planImplementation.escape(alternative, escapedInstance));
        }

        return escapedInstance;
    }

    public Collection<PlanImplementation> getPlanImplementations() {
        return this.planImplementations;
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
                this.getPlanImplementations().size(),
                this.requestedInputSlots, this.servingOutputSlots.stream()
                        .map(Tuple::getField0)
                        .distinct()
                        .collect(Collectors.toList())
        );
    }
}
