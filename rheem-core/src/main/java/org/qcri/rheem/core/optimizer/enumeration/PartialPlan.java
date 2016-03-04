package org.qcri.rheem.core.optimizer.enumeration;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.plan.rheemplan.traversal.AbstractTopologicalTraversal;
import org.qcri.rheem.core.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents a partial execution plan.
 */
public class PartialPlan {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartialPlan.class);

    /**
     * {@link ExecutionOperator}s contained in this instance.
     */
    private final Canonicalizer<ExecutionOperator> operators;

    /**
     * Describes the {@link Channel}s that have been picked between {@link ExecutionOperator}s and how they are
     * implemented.
     */
    private final Map<InputSlot<?>, ChannelChoice> channelChoices;

    /**
     * An enumerated plan is mainly characterized by the {@link OperatorAlternative.Alternative}s that have
     * been picked so far. This member keeps track of them.
     */
    private final Map<OperatorAlternative, OperatorAlternative.Alternative> settledAlternatives =
            new HashMap<>();

    /**
     * The {@link PlanEnumeration} that hosts this instance. Can change over time.
     */
    private PlanEnumeration planEnumeration;

    /**
     * <i>Lazy-initialized.</i> {@link PreliminaryExecutionPlan} representation of this instance.
     */
    private PreliminaryExecutionPlan executionPlan;

    /**
     * Create a new instance.
     */
    PartialPlan(
            PlanEnumeration planEnumeration,
            Map<InputSlot<?>, ChannelChoice> channelChoices,
            Collection<ExecutionOperator> operators) {
        this(planEnumeration, channelChoices, new Canonicalizer<>(operators));
    }

    /**
     * Creates new instance.
     */
    PartialPlan(PlanEnumeration planEnumeration,
                Map<InputSlot<?>, ChannelChoice> channelChoices,
                Collection<ExecutionOperator>... operatorCollections) {
        this(planEnumeration, channelChoices, new Canonicalizer<>());
        for (Collection<ExecutionOperator> operatorCollection : operatorCollections) {
            this.operators.addAll(operatorCollection);
        }
    }

    /**
     * Base constructor.
     */
    private PartialPlan(PlanEnumeration planEnumeration,
                        Map<InputSlot<?>, ChannelChoice> channelChoices,
                        Canonicalizer<ExecutionOperator> operators) {
        this.planEnumeration = planEnumeration;
        this.channelChoices = channelChoices;
        this.operators = operators;
    }


    /**
     * @return the {@link PlanEnumeration} this instance belongs to
     */
    public PlanEnumeration getPlanEnumeration() {
        return this.planEnumeration;
    }

    public void setPlanEnumeration(PlanEnumeration planEnumeration) {
        this.planEnumeration = planEnumeration;
    }

    /**
     * Create a new instance that forms the concatenation of the two.
     *
     * @param that                 instance to join with
     * @param commonScope          {@link OperatorAlternative}s that are selected in both instances
     * @param target               @return the joined instance or {@code null} if the two input instances disagree in some {@link OperatorAlternative}s
     */
    public PartialPlan join(PartialPlan that,
                            List<OperatorAlternative> commonScope,
                            Set<InputSlot<?>> thisToThatConcatenatableInputs,
                            Set<InputSlot<?>> thatToThisConcatenatableInputs,
                            PlanEnumeration target) {

        // Find out if the two plans do not disagree at some point.
        for (OperatorAlternative operatorAlternative : commonScope) {
            final OperatorAlternative.Alternative thisChosenAlternative = this.settledAlternatives.get(operatorAlternative);
            final OperatorAlternative.Alternative thatChosenAlternative = that.settledAlternatives.get(operatorAlternative);
            if (!thatChosenAlternative.equals(thisChosenAlternative)) {
                LOGGER.trace("Cannot combine two partial plans: they disagree in some alternatives");
                return null;
            }
        }

        final PartialPlan partialPlan = new PartialPlan(
                target,
                new HashMap<>(this.channelChoices.size() + that.channelChoices.size() +
                        thatToThisConcatenatableInputs.size() + thisToThatConcatenatableInputs.size()),
                new HashSet<>(this.settledAlternatives.size(), that.settledAlternatives.size())
        );
        partialPlan.operators.addAll(this.operators);
        partialPlan.operators.addAll(that.operators);
        partialPlan.channelChoices.putAll(this.channelChoices);
        partialPlan.channelChoices.putAll(that.channelChoices);
        partialPlan.settledAlternatives.putAll(this.settledAlternatives);
        partialPlan.settledAlternatives.putAll(that.settledAlternatives);

        // Find out how to concatenate the inputs.
        for (InputSlot concatenatableInput : thisToThatConcatenatableInputs) {
            // Find the actually used Slots.
            final OutputSlot<?> execOpOutput = this.findExecutionOperatorOutput(concatenatableInput.getOccupant());
            assert execOpOutput != null;

            Collection<InputSlot<?>> execOpInputs = that.findExecutionOperatorInputs(concatenatableInput);
            assert execOpInputs != null;


        }

        return partialPlan;
    }

    /**
     * Find the {@link InputSlot}s of already picked {@link ExecutionOperator}s that represent the given {@link InputSlot}.
     * <p>Note that we require that this instance either provides all or no {@link ExecutionOperator}s necessary to
     * implement the {@link InputSlot}.</p>
     *
     * @param someInput any {@link InputSlot} of the original {@link RheemPlan}
     * @return the representing {@link InputSlot}s or {@code null} if this instance has no {@link ExecutionOperator}
     * backing the given {@link InputSlot}
     */
    private Collection<InputSlot<?>> findExecutionOperatorInputs(final InputSlot<?> someInput) {
        if (!someInput.getOwner().isExecutionOperator()) {
            final OperatorAlternative owner = (OperatorAlternative) someInput.getOwner();
            final OperatorAlternative.Alternative alternative = this.settledAlternatives.get(owner);
            if (alternative == null) return null;
            @SuppressWarnings("unchecked")
            final Collection<InputSlot<?>> innerInputs = (Collection<InputSlot<?>>) (Collection) alternative.followInput(someInput);
            boolean isWithNull = false;
            Collection<InputSlot<?>> result = null;
            for (InputSlot<?> innerInput : innerInputs) {
                final Collection<InputSlot<?>> resolvedInputs = this.findExecutionOperatorInputs(innerInput);
                if (isWithNull && resolvedInputs != null) {
                    throw new IllegalStateException(String.format("Disallowed that %s is required by two different alternatives.", someInput));
                }
                isWithNull |= resolvedInputs == null;
                if (result == null) {
                    result = resolvedInputs;
                } else {
                    assert resolvedInputs != null;
                    result.addAll(resolvedInputs);
                }
            }
            return result;
        } else {
            Collection<InputSlot<?>> result = new LinkedList<>();
            result.add(someInput);
            return result;
        }

    }

    /**
     * Find the {@link OutputSlot} of an already picked {@link ExecutionOperator} that represents the given {@link OutputSlot}.
     *
     * @param someOutput any {@link InputSlot} of the original {@link RheemPlan}
     * @return the representing {@link OutputSlot} or {@code null} if this instance has no {@link ExecutionOperator}
     * backing the given {@link OutputSlot}
     */
    private OutputSlot<?> findExecutionOperatorOutput(OutputSlot<?> someOutput) {
        while (someOutput != null && !someOutput.getOwner().isExecutionOperator()) {
            final OperatorAlternative owner = (OperatorAlternative) someOutput.getOwner();
            final OperatorAlternative.Alternative alternative = this.settledAlternatives.get(owner);
            if (alternative == null) return null;
            someOutput = alternative.traceOutput(someOutput);
        }
        return someOutput;
    }

    /**
     * Concatenates all {@link PartialPlan}s of the {@code baseEnumeration} via its {@code openOutputSlot}
     * to the {@code targetEnumerations}' {@link PartialPlan}s.
     * All {@link PlanEnumeration}s should be distinct.
     */
    public static Collection<PartialPlan> concatenate(PlanEnumeration baseEnumeration,
                                                      OutputSlot<?> openOutputSlot,
                                                      Map<InputSlot<?>, PlanEnumeration> targetEnumerations) {

        // Sort the PlanEnumerations by their respective open InputSlot or OutputSlot.
        final MultiMap<OutputSlot<?>, PartialPlan> basePlanGroups = new MultiMap<>();
        for (PartialPlan basePlan : baseEnumeration.getPartialPlans()) {
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

        List<Set<Map.Entry<InputSlot<?>, Set<PartialPlan>>>> targetPlanGroupEntrySet =
                RheemCollections.map(targetPlanGroupList, MultiMap::entrySet);
        final Iterable<List<Map.Entry<InputSlot<?>, Set<PartialPlan>>>> targetPlanGroupCrossProduct =
                RheemCollections.streamedCrossProduct(targetPlanGroupEntrySet);
        for (List<Map.Entry<InputSlot<?>, Set<PartialPlan>>> targetPlanGroupEntries : targetPlanGroupCrossProduct) {
            for (Map.Entry<OutputSlot<?>, Set<PartialPlan>> basePlanGroupEntry : basePlanGroups.entrySet()) {
                // TODO: Find appropriate configurations...
                System.out.printf("oha");
                assert false;
            }
        }

        return null;
    }

    /**
     * Escapes the {@link OperatorAlternative} that contains this instance.
     *
     * @param alternative        contains this instance
     * @param newPlanEnumeration will host the new instance
     * @return
     */
    public PartialPlan escape(OperatorAlternative.Alternative alternative, PlanEnumeration newPlanEnumeration) {
        final PartialPlan escapedPartialPlan = new PartialPlan(newPlanEnumeration, this.channelChoices, this.operators);
        escapedPartialPlan.settledAlternatives.putAll(this.settledAlternatives);
        escapedPartialPlan.settledAlternatives.put(alternative.getOperatorAlternative(), alternative);
        return escapedPartialPlan;
    }

    public Canonicalizer<ExecutionOperator> getOperators() {
        return this.operators;
    }

    /**
     * @return those contained {@link ExecutionOperator}s that have a {@link Slot} that is yet to be connected
     * to a further {@link ExecutionOperator} in the further plan enumeration process
     */
    public Collection<ExecutionOperator> getInterfaceOperators() {
        Validate.notNull(this.getPlanEnumeration());
        final Set<OutputSlot> outputSlots = this.getPlanEnumeration().servingOutputSlots.stream()
                .map(Tuple::getField0)
                .distinct()
                .collect(Collectors.toSet());
        final Set<InputSlot<?>> inputSlots = this.getPlanEnumeration().requestedInputSlots;

        return this.operators.stream()
                .filter(operator ->
                        this.allOutermostInputSlots(operator).anyMatch(inputSlots::contains) ||
                                this.allOutermostOutputSlots(operator).anyMatch(outputSlots::contains))
                .collect(Collectors.toList());
    }

    private Stream<OutputSlot> allOutermostOutputSlots(Operator operator) {
        return Arrays.stream(operator.getAllOutputs())
                .flatMap(output -> operator.getOutermostOutputSlots(output).stream());
    }

    private Stream<InputSlot> allOutermostInputSlots(Operator operator) {
        return Arrays.stream(operator.getAllInputs())
                .map(operator::getOutermostInputSlot);
    }

    public PreliminaryExecutionPlan getExecutionPlan() {
        assert this.executionPlan != null;
        return this.executionPlan;
    }

    public PreliminaryExecutionPlan createExecutionPlan() {
        if (this.executionPlan == null) {
            final List<ExecutionOperator> startOperators = this.operators.stream()
                    .filter(this::isStartOperator)
                    .collect(Collectors.toList());
            assert !startOperators.isEmpty() :
                    String.format("Could not find start operators among %s: none provides any of %s.",
                            this.operators, this.planEnumeration.requestedInputSlots);
            final ExecutionPlanCreator executionPlanCreator = new ExecutionPlanCreator(startOperators, this);
            if (executionPlanCreator.traverse()) {
                this.executionPlan = new PreliminaryExecutionPlan(executionPlanCreator.getTerminalTasks());
            }
        }
        return this.executionPlan;
    }

    public PreliminaryExecutionPlan createExecutionPlan(
            ExecutionPlan existingPlan,
            Set<Channel> openChannels,
            Set<ExecutionStage> executedStages) {
        if (this.executionPlan == null) {
            final List<ExecutionOperator> startOperators = this.operators.stream()
                    .filter(this::isStartOperator)
                    .collect(Collectors.toList());
            assert !startOperators.isEmpty() :
                    String.format("Could not find start operators among %s: none provides any of %s.",
                            this.operators, this.planEnumeration.requestedInputSlots);
            try {
                final ExecutionPlanCreator executionPlanCreator = new ExecutionPlanCreator(
                        startOperators, this, existingPlan, openChannels, executedStages);
                if (executionPlanCreator.traverse((Void[]) null)) {
                    this.executionPlan = new PreliminaryExecutionPlan(executionPlanCreator.getTerminalTasks(),
                            executionPlanCreator.getInputChannels());
                }
            } catch (AbstractTopologicalTraversal.AbortException e) {
            }
        }
        return this.executionPlan;
    }

    /**
     * Detects start {@link ExecutionOperator}s.
     * <p>A start {@link ExecutionOperator} has an {@link InputSlot} that is requested by the {@link #planEnumeration}.</p>
     */
    private boolean isStartOperator(ExecutionOperator executionOperator) {
        ForLoop:
        for (InputSlot<?> inputSlot : executionOperator.getOriginal().getAllInputs()) {
            while (inputSlot != null) {
                if (this.planEnumeration.requestedInputSlots.contains(inputSlot)) {
                    continue ForLoop;
                }
                inputSlot = inputSlot.getOwner().getOuterInputSlot(inputSlot);
            }
            return false;
        }
        return true;
    }

    /**
     * Find for a given {@link OperatorAlternative}, which {@link OperatorAlternative.Alternative} has been picked
     * by this instance
     *
     * @param operatorAlternative the {@link OperatorAlternative} in question
     * @return the {@link OperatorAlternative.Alternative} or {@code null} if none has been chosen in this instance
     */
    public OperatorAlternative.Alternative getChosenAlternative(OperatorAlternative operatorAlternative) {
        return this.settledAlternatives.get(operatorAlternative);
    }

    /**
     * Describes the implementation of a connection of an {@link OutputSlot} and an {@link InputSlot}.
     */
    public static class ChannelChoice {

        /**
         * An {InputSlot} of an {@link ExecutionOperator} whose {@link Channel} implementation is described here.
         */
        private final InputSlot<?> inputSlot;

        /**
         * The {@link Channel} that immediately consumes the occupying {@link OutputSlot} of the {@link #inputSlot}.
         */
        private final Channel startChannel;

        /**
         * The {@link Channel} that immediately feed the {@link #inputSlot}.
         */
        private final Channel endChannel;

        public ChannelChoice(InputSlot<?> inputSlot, Channel startChannel, Channel endChannel) {
            this.inputSlot = inputSlot;
            this.startChannel = startChannel;
            this.endChannel = endChannel;
        }
    }
}
