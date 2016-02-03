package org.qcri.rheem.core.optimizer;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.plan.*;
import org.qcri.rheem.core.util.Canonicalizer;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A {@link PlanEnumeration} represents a collection of (partial) execution plans. All these enumerated plans are extracted
 * from the exact same part of a given {@link PhysicalPlan} (which might be a hyperplan). Therefore, the outputs
 * are mapped to this {@link PhysicalPlan} if the plans are partial.
 */
public class PlanEnumeration {

    private static final Logger LOGGER = LoggerFactory.getLogger(PlanEnumeration.class);

    /**
     * Each instance decides on how to implement a set of {@link OperatorAlternative}s.
     */
    private final Set<OperatorAlternative> scope = new HashSet<>();

    /**
     * {@link OperatorAlternative}s whose {@link InputSlot}s depend on an {@link Operator} that is outside of this
     * {@link PlanEnumeration} scope are relevant for pruning and therefore represented here.
     */
    private final Set<InputSlot> requestedInputSlots = new HashSet<>();

    /**
     * {@link OperatorAlternative}s whose {@link OutputSlot}s feed to an {@link Operator} that is outside of this
     * {@link PlanEnumeration} scope are relevant for pruning and therefore represented here.
     */
    private final Set<Tuple<OutputSlot, InputSlot>> servingOutputSlots = new HashSet<>();

//    private final Set<InputSlot> originalInputSlots = new HashSet<>();

//    private final Set<OutputSlot> originalOutputSlots = new HashSet<>();

    private final Set<Operator> terminalOperators = new HashSet<>();

    private final Collection<PartialPlan> partialPlans = new LinkedList<>();

    /**
     * Create an instance for a single {@link ExecutionOperator}.
     *
     * @param operator the mentioned {@link ExecutionOperator}
     * @return the new instance
     */
    static PlanEnumeration createSingleton(ExecutionOperator operator) {
        final PlanEnumeration enumeration = createFor(operator, operator);
        enumeration.add(enumeration.createSingletonPartialPlan(operator));
        return enumeration;
    }

    static PlanEnumeration createFor(OperatorAlternative operatorAlternative) {
        return createFor(operatorAlternative, operatorAlternative);
    }

    static PlanEnumeration createFor(Operator inputOperator, Operator outputOperator) {
        final PlanEnumeration instance = new PlanEnumeration();
        if (inputOperator.isSource()) {
            instance.terminalOperators.add(inputOperator);
        } else {
            for (InputSlot<?> inputSlot : inputOperator.getAllInputs()) {
                instance.requestedInputSlots.add(inputSlot);
            }
        }

        if (outputOperator.isSink()) {
            instance.terminalOperators.add(outputOperator);
        } else {
            for (OutputSlot outputSlot : outputOperator.getAllOutputs()) {
                List<InputSlot> inputSlots = outputSlot.getOccupiedSlots();
                if (inputSlots.isEmpty()) {
                    inputSlots = Collections.singletonList(null);
                    for (InputSlot inputSlot : inputSlots) {
                        instance.servingOutputSlots.add(new Tuple<>(outputSlot, inputSlot));
                    }
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

        if (!instance1.terminalOperators.equals(instance2.terminalOperators)) {
            throw new IllegalArgumentException("Terminal operators are not matching.");
        }
    }

    private PlanEnumeration() {
    }

    /**
     * Create a new instance that represents the concatenation of two further instances.
     */
    private PlanEnumeration(PlanEnumeration pe1, PlanEnumeration pe2) {
        // Figure out if the two instance can be concatenated in the first place.
        // To this end, there muse be some remaining OutputSlot of this instance that connects to an InputSlot
        // of the other instance. At first, we collect these touchpoints.
        final Set<InputSlot> connectedInputSlots = pe1.collectDownstreamTouchpoints(pe2);
        if (connectedInputSlots.isEmpty()) {
            LOGGER.warn("Could not find a (directed) point of touch when joining to instances.");
        }

        final List<OperatorAlternative> commonScope = pe1.intersectScopeWith(pe2);

        // Determine the new input slots.
//        Counter<InputSlot> inputSlotRequestCounter = new Counter<>();
//        pe1.requestedInputSlots.forEach(inputSlotRequestCounter::increment);
//        pe2.requestedInputSlots.forEach(inputSlotRequestCounter::increment);


        // Add all slots that are requested by both input instances.
        this.requestedInputSlots.addAll(pe1.requestedInputSlots);
        this.requestedInputSlots.retainAll(pe2.requestedInputSlots);

        // Add all slots that are requested by one of the input instances and is not in the common scope.
        pe1.requestedInputSlots.stream()
                .filter(inputSlot -> !commonScope.contains(inputSlot.getOwner()))
                .forEach(this.requestedInputSlots::add);
        pe2.requestedInputSlots.stream()
                .filter(inputSlot -> !commonScope.contains(inputSlot.getOwner()))
                .forEach(this.requestedInputSlots::add);

        // Delete served input slots
        pe1.servingOutputSlots.removeAll(connectedInputSlots);


        // Add all slots that are provided by both input instances.
        this.servingOutputSlots.addAll(pe1.servingOutputSlots);
        this.servingOutputSlots.retainAll(pe2.servingOutputSlots);

        // Add all slots that are requested by one of the input instances and are not in the common scope.
        pe1.servingOutputSlots.stream()
                .filter(outputSlotInputSlotTuple -> !commonScope.contains(outputSlotInputSlotTuple.field0))
                .forEach(this.servingOutputSlots::add);
        pe2.servingOutputSlots.stream()
                .filter(outputSlotInputSlotTuple -> !commonScope.contains(outputSlotInputSlotTuple.field0))
                .forEach(this.servingOutputSlots::add);

        // Delete serving input slots.
        this.servingOutputSlots.removeIf(outputSlotInputSlotTuple -> connectedInputSlots.contains(outputSlotInputSlotTuple.getField1()));


        // Determine the new scope of the two enumerations.
        this.scope.addAll(pe1.scope);
        this.scope.addAll(pe2.scope);

        // Concatenate each possible combination of plans.
        joinPartialPlansUsingNestedLoops(pe1, pe2, commonScope);
        // TODO: use a hybrid hash join if the scope is not empty.
    }

    private void joinPartialPlansUsingNestedLoops(PlanEnumeration pe1, PlanEnumeration pe2, List<OperatorAlternative> commonScope) {
        for (PartialPlan plan1 : pe1.partialPlans) {
            for (PartialPlan plan2 : pe2.partialPlans) {
                final PartialPlan newPartialPlan = plan1.join(plan2, commonScope);
                if (newPartialPlan != null) {
                    this.add(newPartialPlan);
                }
            }
        }
    }


    /**
     * Concatenate two instances. This potentially increases number of {@link #partialPlans} to the product
     * of the numbers of partial plans of the two instances.
     *
     * @param that instance to join
     * @return a new instance representing the concatenation
     */
    public PlanEnumeration join(PlanEnumeration that) {
        return new PlanEnumeration(this, that);
    }

    private Set<InputSlot> collectDownstreamTouchpoints(PlanEnumeration that) {
        Set<InputSlot> touchpoints = this.servingOutputSlots.stream()
                .map(Tuple::getField1)
                .collect(Collectors.toCollection(HashSet::new));
        touchpoints.retainAll(that.requestedInputSlots);
        return touchpoints;
    }

    private List<OperatorAlternative> intersectScopeWith(PlanEnumeration that) {
        return this.scope.stream().filter(that.scope::contains).collect(Collectors.toList());
    }

    /**
     * Add a {@link PlanEnumeration.PartialPlan} to this instance.
     *
     * @param partialPlan to be added
     */
    public void add(PartialPlan partialPlan) {
        // TODO: Check if the plan conforms to this instance.
        this.partialPlans.add(partialPlan);
    }

    /**
     * Creates a new instance for exactly one {@link ExecutionOperator}.
     *
     * @param executionOperator will be wrapped in the new instance
     * @return the new instance
     */
    private PartialPlan createSingletonPartialPlan(ExecutionOperator executionOperator) {
        return new PartialPlan(
//                SlotMapping.createIdentityMapping(executionOperator),
                Collections.singletonList(executionOperator)
        );

    }

    public boolean isComprehensive() {
        return this.servingOutputSlots.isEmpty() && this.requestedInputSlots.isEmpty();
    }

    public void subsume(PlanEnumeration that) {
        assertMatchingInterface(this, that);
        this.scope.addAll(that.scope);
        that.partialPlans.forEach(partialPlan -> {
            this.partialPlans.add(partialPlan);
            partialPlan.setPlanEnumeration(this);
        });
        that.partialPlans.clear();
    }

    /**
     * Create a new instance that equals this instance but redirects using the given {@link SlotMapping}
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
        for (Tuple<OutputSlot, InputSlot> link : this.servingOutputSlots) {
            if (link.field1 != null) {
                throw new IllegalStateException("Cannot escape a connected output slot.");
            }
            final Collection<OutputSlot> resolvedOutputSlots =
                    alternative.getSlotMapping().resolveDownstream(link.field0);
            for (OutputSlot escapedOutput : resolvedOutputSlots) {
                final List<InputSlot> occupiedInputs = escapedOutput.getOccupiedSlots();
                if (occupiedInputs.isEmpty()) {
                    escapedInstance.servingOutputSlots.add(new Tuple<>(escapedOutput, null));
                } else {
                    for (InputSlot inputSlot : occupiedInputs) {
                        escapedInstance.servingOutputSlots.add(new Tuple<>(escapedOutput, inputSlot));
                    }
                }
            }
        }

        // Escape the terminal operators.
        if (this.terminalOperators.size() == 1) {
            // If there is a terminal operator (a sink or a source), then the enclosing OperatorAlternative should be
            // one as well.
            final Operator terminalOperator = this.terminalOperators.stream().findAny().get();
            if ((terminalOperator.isSink() ^ operatorAlternative.isSink()) ||
                    (terminalOperator.isSource() ^ terminalOperator.isSource()) ||
                    !(terminalOperator.isSource() ^ terminalOperator.isSink())) {
                throw new IllegalStateException("Operator alternative and inner operators should be consistently either a source or a sink.");
            }
            escapedInstance.terminalOperators.add(operatorAlternative);
        } else if (this.terminalOperators.size() > 1) {
            throw new IllegalStateException("More than one terminal operator cannot be escaped!");
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

//    public PlanEnumeration join(PlanEnumeration that) {
//        this.scope.addAll(that.scope);
//        this.requestedInputSlots.addAll(that.requestedInputSlots);
//        this.servingOutputSlots.addAll(that.servingOutputSlots);
//        this.terminalOperators.addAll(that.terminalOperators);
//
//        final List<OperatorAlternative> commonScope = this.intersectScopeWith(that);
//
//    }

    /**
     * Represents a partial execution plan.
     */
    public static class PartialPlan {

//        /**
//         * Maps {@link #originalInputSlots} and {@link #originalOutputSlots} to this enumeration element.
//         */
//        private final SlotMapping slotMapping;

        /**
         * {@link ExecutionOperator}s contained in this instance.
         */
        private final Canonicalizer<ExecutionOperator> operators;

        /**
         * An enumerated plan is mainly characterized by the {@link OperatorAlternative.Alternative}s that have
         * been picked so far. This member keeps track of them.
         */
        private final Map<OperatorAlternative, OperatorAlternative.Alternative> settledAlternatives =
                new HashMap<>();

        private PlanEnumeration planEnumeration;

        /**
         * Create a new instance.
         */
        public PartialPlan(
                PlanEnumeration planEnumeration,
                Collection<ExecutionOperator> operators) {
//            this(planEnumeration, operators, null);
            this.planEnumeration = planEnumeration;
            this.operators = new Canonicalizer<>(operators);
        }

//        /**
//         * Create a new instance.
//         */
//        public PartialPlan(
//                PlanEnumeration planEnumeration,
//                Collection<ExecutionOperator> operators,
//                SlotMapping slotMapping) {
//            this.slotMapping = slotMapping;
//            this.operators = new Canonicalizer<>(operators);
//            this.planEnumeration = planEnumeration;
//        }

        private PartialPlan(Collection<ExecutionOperator>... operatorCollections) {
            this.operators = new Canonicalizer<>();
            for (Collection<ExecutionOperator> operatorCollection : operatorCollections) {
                this.operators.addAll(operatorCollection);
            }
//            this.slotMapping = null;
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
         * @param that        instance to join
         * @param commonScope
         */
        public PartialPlan join(PartialPlan that, List<OperatorAlternative> commonScope) {

            // Find out if the two plans do not disagree at some point.
            for (OperatorAlternative operatorAlternative : commonScope) {
                final OperatorAlternative.Alternative thisChosenAlternative = this.settledAlternatives.get(operatorAlternative);
                final OperatorAlternative.Alternative thatChosenAlternative = that.settledAlternatives.get(operatorAlternative);
                if (!thatChosenAlternative.equals(thisChosenAlternative)) {
                    LOGGER.info("Cannot combine two partial plans: they disagree in some alternatives");
                    return null;
                }
            }

            // Find out, how the two partial plans can be concatenated in the first place.
//            final Map<InputSlot, OutputSlot> touchpoints = this.slotMapping.compose(that.slotMapping);
//            if (touchpoints.isEmpty()) {
//                LOGGER.info("Cannot combine two partial plans: no touchpoint between them");
//                return null;
//            }

            // Create a new instance top-down/upstream.
//            final Collection<ExecutionOperator> thatSinkLikeOperators = that.findSinkLikeOperators();
            // TODO


            final PartialPlan partialPlan = new PartialPlan();
            partialPlan.operators.addAll(this.operators);
            partialPlan.operators.addAll(that.operators);
            partialPlan.settledAlternatives.putAll(this.settledAlternatives);
            partialPlan.settledAlternatives.putAll(that.settledAlternatives);

            return partialPlan;
        }

        private Collection<ExecutionOperator> findSinkLikeOperators() {
            return this.operators.stream()
                    .filter(op -> op.isSink() || Arrays.stream(op.getAllOutputs())
                            .allMatch(outputSlot -> outputSlot.getOccupiedSlots().isEmpty()))
                    .collect(Collectors.toList());
        }

        public PartialPlan escape(OperatorAlternative.Alternative alternative, PlanEnumeration newPlanEnumeration) {
            final PartialPlan escapedPartialPlan = new PartialPlan(newPlanEnumeration, this.operators);
            escapedPartialPlan.settledAlternatives.putAll(this.settledAlternatives);
            escapedPartialPlan.settledAlternatives.put(alternative.getOperatorAlternative(), alternative);
            return escapedPartialPlan;
        }

        public Canonicalizer<ExecutionOperator> getOperators() {
            return operators;
        }

        public PhysicalPlan toPhysicalPlan() {
            Map<OutputSlot<?>, ExecutionOperator> copiedOutputProviders = new HashMap<>();
            Map<OutputSlot<?>, Collection<InputSlot<?>>> copiedOutputRequesters = new HashMap<>();

            Collection<ExecutionOperator> sinks = new LinkedList<>();
            for (ExecutionOperator pickedOperator : this.operators) {
                final ExecutionOperator copy = pickedOperator.copy();

                // Connect the InputSlots or issue an connecting request.
                for (InputSlot<?> originalInput : pickedOperator.getAllInputs()) {
                    final InputSlot<?> originalOuterInput = pickedOperator.getOutermostInputSlot(originalInput);
                    final OutputSlot<?> originalRequestedOutput = originalOuterInput.getOccupant();
                    Validate.notNull(originalRequestedOutput, "Outermost %s does not have an occupant.", originalOuterInput);
                    final ExecutionOperator copiedOutputProvider = copiedOutputProviders.get(originalRequestedOutput);
                    if (copiedOutputProvider != null) {
                        copiedOutputProvider.connectTo(originalRequestedOutput.getIndex(),
                                copy,
                                originalInput.getIndex());
                    } else {
                        RheemCollections.put(copiedOutputRequesters,
                                originalRequestedOutput,
                                copy.getInput(originalInput.getIndex()));
                    }
                }

                // Connect the OutputSlots and offer further connections.
                for (OutputSlot<?> originalOutput : pickedOperator.getAllOutputs()) {
                    Collection<OutputSlot<Object>> originalOuterOutputs =
                            pickedOperator.getOutermostOutputSlots(originalOutput.unchecked());
                    for (OutputSlot<Object> originalOuterOutput : originalOuterOutputs) {
                        copiedOutputProviders.put(originalOuterOutput, copy);
                        final Collection<InputSlot<?>> connectableOutputRequesters =
                                copiedOutputRequesters.remove(originalOuterOutput);
                        if (connectableOutputRequesters != null) {
                            for (InputSlot<?> outputRequester : connectableOutputRequesters) {
                                copy.connectTo(originalOutput.getIndex(),
                                        outputRequester.getOwner(),
                                        outputRequester.getIndex());
                            }
                        }
                    }
                }

                // Remember sinks.
                if (copy.isSink()) {
                    sinks.add(copy);
                }
            }
            Validate.isTrue(copiedOutputRequesters.isEmpty());
            Validate.isTrue(!sinks.isEmpty());

            final PhysicalPlan physicalPlan = new PhysicalPlan();
            sinks.forEach(physicalPlan::addSink);
            return physicalPlan;
        }
    }

}
