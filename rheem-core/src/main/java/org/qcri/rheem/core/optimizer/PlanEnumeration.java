package org.qcri.rheem.core.optimizer;

import org.qcri.rheem.core.plan.*;
import org.qcri.rheem.core.util.Canonicalizer;
import org.qcri.rheem.core.util.Counter;
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
    private final Set<InputSlot> inputSlots = new HashSet<>();

    /**
     * {@link OperatorAlternative}s whose {@link OutputSlot}s feed to an {@link Operator} that is outside of this
     * {@link PlanEnumeration} scope are relevant for pruning and therefore represented here.
     */
    private final Counter<OutputSlot> outputSlotCounter = new Counter<>();

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
                instance.inputSlots.add(inputSlot);
            }
        }

        if (outputOperator.isSink()) {
            instance.terminalOperators.add(outputOperator);
        } else {
            for (OutputSlot<?> outputSlot : outputOperator.getAllOutputs()) {
                instance.outputSlotCounter.increment(outputSlot);
            }
        }
        return instance;
    }

    private static void assertMatchingInterface(PlanEnumeration instance1, PlanEnumeration instance2) {
        if (!instance1.inputSlots.equals(instance2.inputSlots)) {
            throw new IllegalArgumentException("Input slots are not matching.");
        }

        if (!instance1.outputSlotCounter.equals(instance2.outputSlotCounter)) {
            throw new IllegalArgumentException("Output slots are not matching.");
        }

        if (!instance1.terminalOperators.equals(instance2.terminalOperators)) {
            throw new IllegalArgumentException("Terminal operators are not matching.");
        }
    }

    private PlanEnumeration() {
    }

    private PlanEnumeration(PlanEnumeration pe1, PlanEnumeration pe2) {
        // Figure out if the two instance can be concatenated in the first place.
        // To this end, there muse be some remaining OutputSlot of this instance that connects to an InputSlot
        // of the other instance. At first, we collect these touchpoints.
        Map<InputSlot, OutputSlot> touchpoints = pe1.collectDownstreamTouchpoints(pe2);
        if (touchpoints.isEmpty()) {
            throw new IllegalArgumentException("Cannot concatenate plan enumerations: no (directed) point of touch");
        }

        // Determine the new input slots.
        this.inputSlots.addAll(pe1.inputSlots);
        this.inputSlots.addAll(pe2.inputSlots);
        this.inputSlots.removeAll(touchpoints.keySet());

        // Determine the new output slots.
        this.outputSlotCounter.addAll(pe1.outputSlotCounter);
        this.outputSlotCounter.addAll(pe2.outputSlotCounter);
        touchpoints.values().forEach(this.outputSlotCounter::decrement);

        // Determine the common scope of the two enumerations, because plans might conflig here.
        final List<OperatorAlternative> commonScope = pe1.intersectScopeWith(pe2);
        this.scope.addAll(pe1.scope);
        this.scope.addAll(pe2.scope);

        // Concatenate each possible combination of plans.
        for (PartialPlan plan1 : pe1.partialPlans) {
            for (PartialPlan plan2 : pe2.partialPlans) {
                final PartialPlan newPartialPlan = plan1.concatenate(plan2, touchpoints, commonScope);
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
     * @param that instance to concatenate
     * @return a new instance representing the concatenation
     */
    public PlanEnumeration concatenate(PlanEnumeration that) {
        return new PlanEnumeration(this, that);
    }

    private Map<InputSlot, OutputSlot> collectDownstreamTouchpoints(PlanEnumeration that) {
        Map<InputSlot, OutputSlot> touchpoints = new HashMap<>();
        for (InputSlot inputSlot : that.inputSlots) {
            final OutputSlot occupant = inputSlot.getOccupant();
            if (occupant != null && this.outputSlotCounter.get(occupant) > 0) {
                touchpoints.put(inputSlot, occupant);
            }
        }
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
        return this.outputSlotCounter.isEmpty() && this.inputSlots.isEmpty();
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
        for (InputSlot inputSlot : this.inputSlots) {
            final InputSlot escapedInput = alternative.getSlotMapping().resolveUpstream(inputSlot);
            if (escapedInput != null) {
                escapedInstance.inputSlots.add(escapedInput);
            }
        }

        // Escape the output slots.
        for (Map.Entry<OutputSlot, Integer> count : this.outputSlotCounter) {
            final OutputSlot<Object> thisOutput = count.getKey().unchecked();
            for (OutputSlot<Object> escapedOutput : alternative.getSlotMapping().resolveDownstream(thisOutput)) {
                escapedInstance.outputSlotCounter.add(escapedOutput, count.getValue());
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
         * @param that        instance to concatenate
         * @param touchpoints @return the concatenated instance or {@code null} if concatenation is impossible
         * @param commonScope
         */
        public PartialPlan concatenate(PartialPlan that,
                                       Map<InputSlot, OutputSlot> touchpoints, List<OperatorAlternative> commonScope) {

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
    }

}
