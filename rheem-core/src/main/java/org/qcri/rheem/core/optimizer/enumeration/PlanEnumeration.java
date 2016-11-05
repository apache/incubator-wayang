package org.qcri.rheem.core.optimizer.enumeration;

import de.hpi.isg.profiledb.store.model.TimeMeasurement;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.channels.ChannelConversionGraph;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.platform.Junction;
import org.qcri.rheem.core.util.MultiMap;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger logger = LoggerFactory.getLogger(PlanEnumeration.class);

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
                                       Collection<Channel> openChannels,
                                       Map<InputSlot<?>, PlanEnumeration> targetEnumerations,
                                       OptimizationContext optimizationContext,
                                       TimeMeasurement enumerationMeasurement) {

        // Check the parameters' validity.
        assert this.getServingOutputSlots().stream()
                .map(Tuple::getField0)
                .anyMatch(openOutputSlot::equals)
                : String.format("Cannot concatenate %s: it is not a served output.", openOutputSlot);
        assert !targetEnumerations.isEmpty();

        final TimeMeasurement concatenationMeasurement = enumerationMeasurement == null ?
                null :
                enumerationMeasurement.start("Concatenation");

        if (logger.isInfoEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Concatenating ").append(this.getPlanImplementations().size());
            for (PlanEnumeration targetEnumeration : targetEnumerations.values()) {
                sb.append("x").append(targetEnumeration.getPlanImplementations().size());
            }
            sb.append(" plan implementations.");
            logger.debug(sb.toString());
        }

        // Prepare the result instance from this instance.
        PlanEnumeration result = new PlanEnumeration();
        result.scope.addAll(this.getScope());
        result.requestedInputSlots.addAll(this.getRequestedInputSlots());
        result.servingOutputSlots.addAll(this.getServingOutputSlots());
        result.executedTasks.putAll(this.getExecutedTasks());

        // Update the result instance from the target instances.
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

        // Create the PlanImplementations.
        result.planImplementations.addAll(this.concatenatePartialPlans(
                openOutputSlot,
                openChannels,
                targetEnumerations,
                optimizationContext,
                result,
                concatenationMeasurement
        ));

        logger.debug("Created {} plan implementations.", result.getPlanImplementations().size());
        if (concatenationMeasurement != null) concatenationMeasurement.stop();
        return result;
    }

    /**
     * Concatenates all {@link PlanImplementation}s of the {@code baseEnumeration} via its {@code openOutputSlot}
     * to the {@code targetEnumerations}' {@link PlanImplementation}s.
     * All {@link PlanEnumeration}s should be distinct.
     */
    private Collection<PlanImplementation> concatenatePartialPlans(OutputSlot<?> openOutputSlot,
                                                                   Collection<Channel> openChannels,
                                                                   Map<InputSlot<?>, PlanEnumeration> targetEnumerations,
                                                                   OptimizationContext optimizationContext,
                                                                   PlanEnumeration concatenationEnumeration,
                                                                   TimeMeasurement concatenationMeasurement) {
        final Job job = optimizationContext.getJob();
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.getOperatorContext(openOutputSlot.getOwner());
        boolean isRequestBreakpoint = job.isRequestBreakpointFor(openOutputSlot, operatorContext);
        return this.concatenatePartialPlansBatchwise(
                openOutputSlot,
                openChannels,
                targetEnumerations,
                optimizationContext,
                isRequestBreakpoint,
                concatenationEnumeration,
                concatenationMeasurement
        );
    }

    /**
     * Concatenates {@link PlanEnumeration}s by batchwise processing of {@link PlanImplementation}s. All {@link PlanImplementation}s
     * that share a certain implementation of the {@code openOutputSlot} or its fed {@link InputSlot}s are grouped
     * into combinations so that we avoid to seek redundant {@link Junction}s.
     *
     * @param openOutputSlot           of this instance to be concatenated
     * @param targetEnumerations       whose {@link InputSlot}s should be concatenated with the {@code openOutputSlot}
     * @param optimizationContext      provides concatenation information
     * @param concatenationEnumeration to which the {@link PlanImplementation}s should be added
     * @param concatenationMeasurement
     * @param isRequestBreakpoint      whether a breakpoint-capable {@link Channel} should be inserted
     * @return the concatenated {@link PlanImplementation}s
     */
    private Collection<PlanImplementation> concatenatePartialPlansBatchwise(
            OutputSlot<?> openOutputSlot,
            Collection<Channel> openChannels,
            Map<InputSlot<?>, PlanEnumeration> targetEnumerations,
            OptimizationContext optimizationContext,
            boolean isRequestBreakpoint,
            PlanEnumeration concatenationEnumeration,
            TimeMeasurement concatenationMeasurement) {

        // Preparatory initializations.
        final ChannelConversionGraph channelConversionGraph = optimizationContext.getChannelConversionGraph();

        // Group the base and target PlanImplementations by their operator.
        final MultiMap<OutputSlot<?>, Tuple<PlanImplementation, PlanImplementation>> basePlanGroups =
                this.groupImplementationsByOutput(openOutputSlot);
        final List<MultiMap<Set<InputSlot<?>>, PlanImplementation>> targetPlanGroupList =
                groupImplementationsByInput(targetEnumerations);


        // Allocate result collector.
        Collection<PlanImplementation> result = new LinkedList<>();

        // Iterate all InputSlot/OutputSlot combinations.
        List<Set<Map.Entry<Set<InputSlot<?>>, Set<PlanImplementation>>>> targetPlanGroupEntrySet =
                RheemCollections.map(targetPlanGroupList, MultiMap::entrySet);
        final Iterable<List<Map.Entry<Set<InputSlot<?>>, Set<PlanImplementation>>>> targetPlanGroupCrossProduct =
                RheemCollections.streamedCrossProduct(targetPlanGroupEntrySet);
        for (List<Map.Entry<Set<InputSlot<?>>, Set<PlanImplementation>>> targetPlanGroupEntries : targetPlanGroupCrossProduct) {
            // Flatten the requested InputSlots.
            final List<InputSlot<?>> inputs = targetPlanGroupEntries.stream()
                    .map(Map.Entry::getKey)
                    .flatMap(Collection::stream)
                    .distinct()
                    .collect(Collectors.toCollection(() -> new ArrayList<>(4)));

            for (Map.Entry<OutputSlot<?>, Set<Tuple<PlanImplementation, PlanImplementation>>> basePlanGroupEntry : basePlanGroups.entrySet()) {
                final OutputSlot<?> output = basePlanGroupEntry.getKey();
                final PlanImplementation innerPlanImplementation = RheemCollections.getAny(basePlanGroupEntry.getValue()).getField1();
                // The output should reside in the same OptimizationContext in all PlanImplementations.
                assert basePlanGroupEntry.getValue().stream()
                        .map(Tuple::getField1)
                        .map(PlanImplementation::getOptimizationContext)
                        .collect(Collectors.toSet()).size() == 1;

                // Construct a Junction between the ExecutionOperators.
                final Operator outputOperator = output.getOwner();
                assert outputOperator.isExecutionOperator()
                        : String.format("Expected execution operator, found %s.", outputOperator);
                TimeMeasurement channelConversionMeasurement = concatenationMeasurement == null ?
                        null : concatenationMeasurement.start("Channel Conversion");
                final Junction junction = openChannels == null || openChannels.isEmpty() ?
                        channelConversionGraph.findMinimumCostJunction(
                                output,
                                inputs,
                                innerPlanImplementation.getOptimizationContext(),
                                isRequestBreakpoint
                        ) :
                        channelConversionGraph.findMinimumCostJunction(
                                output,
                                openChannels,
                                inputs,
                                innerPlanImplementation.getOptimizationContext());
                if (channelConversionMeasurement != null) channelConversionMeasurement.stop();
                if (junction == null) continue;

                // If we found a junction, then we can enumerate all PlanImplementation combinations.
                final List<Set<PlanImplementation>> targetPlans = RheemCollections.map(targetPlanGroupEntries, Map.Entry::getValue);
                for (List<PlanImplementation> targetPlanList : RheemCollections.streamedCrossProduct(targetPlans)) {
                    for (Tuple<PlanImplementation, PlanImplementation> basePlanImplementations : basePlanGroupEntry.getValue()) {
                        PlanImplementation basePlan = basePlanImplementations.getField0();
                        PlanImplementation concatenatedPlan = basePlan.concatenate(targetPlanList, junction, basePlan, concatenationEnumeration);
                        if (concatenatedPlan != null) {
                            result.add(concatenatedPlan);
                        }
                    }
                }
            }
        }
        return result;
    }

    /**
     * Groups all {@link #planImplementations} by their {@link ExecutionOperator}s' {@link OutputSlot}s for the
     * {@code output}. Additionally preserves the very (nested) {@link PlanImplementation} in that {@code output} resides.
     *
     * @param output a (possibly top-level) {@link OutputSlot} that should be connected
     * @return a mapping that represents each element {@link #planImplementations} by a key value pair
     * {@code (implementing OutputSlots -> (PlanImplementation, nested PlanImplementation)}
     */
    private MultiMap<OutputSlot<?>, Tuple<PlanImplementation, PlanImplementation>>
    groupImplementationsByOutput(OutputSlot<?> output) {
        // Sort the PlanEnumerations by their respective open InputSlot or OutputSlot.
        final MultiMap<OutputSlot<?>, Tuple<PlanImplementation, PlanImplementation>> basePlanGroups =
                new MultiMap<>();
        // Find and validate implementing OutputSlots.
        for (PlanImplementation basePlanImplementation : this.getPlanImplementations()) {
            final Collection<Tuple<OutputSlot<?>, PlanImplementation>> execOpOutputsWithContext =
                    basePlanImplementation.findExecutionOperatorOutputWithContext(output);
            final Tuple<OutputSlot<?>, PlanImplementation> execOpOutputWithCtx =
                    RheemCollections.getSingleOrNull(execOpOutputsWithContext);
            assert execOpOutputsWithContext != null && !execOpOutputsWithContext.isEmpty()
                    : String.format("No outputs found for %s.", output);

            basePlanGroups.putSingle(
                    execOpOutputWithCtx.getField0(),
                    new Tuple<>(basePlanImplementation, execOpOutputWithCtx.getField1())
            );
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
        return new PlanImplementation(
                this,
                new HashMap<>(0),
                Collections.singletonList(executionOperator),
                optimizationContext
        );
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
        return this.toIOString();
    }

    @SuppressWarnings("unused")
    private String toIOString() {
        return String.format("%s[%dx, inputs=%s, outputs=%s]", this.getClass().getSimpleName(),
                this.getPlanImplementations().size(),
                this.requestedInputSlots, this.servingOutputSlots.stream()
                        .map(Tuple::getField0)
                        .distinct()
                        .collect(Collectors.toList())
        );
    }

    @SuppressWarnings("unused")
    private String toScopeString() {
        return String.format("%s[%dx %s]", this.getClass().getSimpleName(),
                this.getPlanImplementations().size(),
                this.scope
        );
    }
}
