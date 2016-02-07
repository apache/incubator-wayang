package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * The plan partitioner recursively dissects a {@link PhysicalPlan} into {@link PlanEnumeration}s and then assembles
 * them.
 */
public class PlanEnumerator {

    /**
     * Logger.
     */
    private Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * {@link CardinalityEstimate}s that can be used to assess plans.
     */
    private final Map<ExecutionOperator, TimeEstimate> timeEstimates;

    /**
     * {@link PlanEnumerator.OperatorActivation}s that can be activated and should be followed to create branches.
     */
    private final List<OperatorActivation> activatedOperators = new LinkedList<>();

    /**
     * When this instance enumerates an {@link OperatorAlternative.Alternative}, then this field helps to
     * create correct {@link PlanEnumeration}s by mapping the enumerates {@link Operator}s to the {@link OperatorAlternative}'s
     * slots.
     */
    private OperatorAlternative.Alternative alternative = null;

    /**
     * Maintain {@link PlanEnumerator.OperatorActivation} for {@link Operator}s.
     */
    private final Map<Operator, OperatorActivation> activationCollector = new HashMap<>();

    /**
     * This instance will put all completed {@link PlanEnumeration}s (which did not cause an activation) here.
     */
    private final Collection<PlanEnumeration> completedEnumerations = new LinkedList<>();

    /**
     * {@link PlanEnumerationPruningStrategy}s to be applied while enumerating.
     */
    private final Collection<PlanEnumerationPruningStrategy> pruningStrategies;

    /**
     * Creates a new instance.
     *
     * @param physicalPlan a hyperplan that should be used for enumeration.
     */
    public PlanEnumerator(PhysicalPlan physicalPlan, Map<ExecutionOperator, TimeEstimate> timeEstimates) {
        this(physicalPlan.collectReachableTopLevelSources(), timeEstimates, new LinkedList<>());
    }

    /**
     * Fork constructor.
     */
    private PlanEnumerator(OperatorAlternative.Alternative alternative,
                           Map<ExecutionOperator, TimeEstimate> timeEstimates,
                           Collection<PlanEnumerationPruningStrategy> pruningStrategies) {
        this(findStartOperators(alternative), timeEstimates, pruningStrategies);
        this.alternative = alternative;
    }

    /**
     * Basic constructor.
     */
    private PlanEnumerator(Collection<Operator> startOperators,
                           Map<ExecutionOperator, TimeEstimate> timeEstimates,
                           Collection<PlanEnumerationPruningStrategy> pruningStrategies) {
        startOperators.stream()
                .map(PlanEnumerator.OperatorActivation::new)
                .forEach(this.activatedOperators::add);
        this.timeEstimates = timeEstimates;
        this.pruningStrategies = pruningStrategies;
    }

    /**
     * Collect all the start operators of an {@link OperatorAlternative.Alternative}.
     *
     * @param alternative the {@link OperatorAlternative.Alternative} to investigate
     * @return the start {@link Operator}s
     */
    private static Collection<Operator> findStartOperators(OperatorAlternative.Alternative alternative) {
        Operator alternativeOperator = alternative.getOperator();
        if (alternativeOperator.isSubplan()) {
            Subplan subplan = (Subplan) alternativeOperator;
            return subplan.collectInputOperators();
        } else {
            return Collections.singleton(alternativeOperator);
        }
    }

    /**
     * Execute the enumeration.
     */
    public void run() {
        this.logger.debug("Enumerating with the activated operators {}.", this.activatedOperators.stream()
                .map(OperatorActivation::getOperator)
                .collect(Collectors.toList()));

        while (!this.activatedOperators.isEmpty()) {
            // Create branches for all activated operators.
            this.activatedOperators.forEach(this::enumerateBranchFrom);

            // Clean up the old activated operators and spawn the new ones.
            updateActivatedOperator();
        }
    }

    /**
     * Enumerate plans from the branch that starts at the given node.
     *
     * @param operatorActivation the activated {@link PlanEnumerator.OperatorActivation}
     */
    private void enumerateBranchFrom(OperatorActivation operatorActivation) {
        // Start with the activated operator.
        Operator currentOperator = operatorActivation.activatableOperator;
        logger.debug("Creating new branch starting at {}.", currentOperator);

        List<Operator> branch = determineBranch(currentOperator);

//        PlanEnumeration branchEnumeration = PlanEnumeration.createFor(
//                branch.get(0),
//                branch.get(branch.size() - 1));

        // Go over the branch and PlanEnumeration for it.
        PlanEnumeration branchEnumeration = enumerateBranch(branch);
        if (branchEnumeration == null) {
            return;
        }
        this.prune(branchEnumeration);

        // Build the complete enumeration from head to toe.
        PlanEnumeration completeEnumeration = branchEnumeration;
        for (PlanEnumeration inputEnumeration : operatorActivation.activationCollector) {
            // Inputs might not always be available, e.g., when enumerating an alternative.
            if (inputEnumeration != null) {
                completeEnumeration = inputEnumeration.join(completeEnumeration);
            }

            this.prune(completeEnumeration);
        }

        // Once we stopped, activate all successive operators.
        if (!activateDownstreamOperators(branch, completeEnumeration)) {
            this.completedEnumerations.add(completeEnumeration);
        }
    }

    /**
     * Determine the branch (straight of operators) that begins at the given {@link Operator}.
     *
     * @param startOperator starts the branch
     * @return the {@link Operator}s of the branch in their order of appearance
     */
    private List<Operator> determineBranch(Operator startOperator) {
        List<Operator> branch = new LinkedList<>();
        Operator currentOperator = startOperator;
        while (true) {
            this.logger.debug("Adding {} to the branch.", currentOperator);

//            if (currentOperator.isAlternative()) {
//                OperatorAlternative operatorAlternative = (OperatorAlternative) currentOperator;
//                for (OperatorAlternative.Alternative alternative : operatorAlternative.getAlternatives()) {
//                    System.out.println("Nesting branch because of alternative...");
//                    new PlanEnumerator(findStartOperators(alternative)).run();
//                    System.out.println("Finished nesting.");
//                }
//            }

            branch.add(currentOperator);
            // Try to advance. This requires certain conditions, though.
            if (currentOperator.getNumOutputs() != 1) {
                this.logger.debug("Stopping branch, because operator does not have exactly one output.");
                break;
            }
            if (currentOperator.getOutput(0).getOccupiedSlots().size() != 1) {
                this.logger.debug("Stopping branch, because operator does not feed exactly one operator.");
                break;
            }
            Operator nextOperator = currentOperator.getOutput(0).getOccupiedSlots().get(0).getOwner();
            if (nextOperator.getNumInputs() != 1) {
                this.logger.debug("Stopping branch, because next operator does not have exactly one input.");
                break;
            }

            currentOperator = nextOperator;
        }
        this.logger.debug("Determined branch : {}.", currentOperator);

        return branch;
    }

    private PlanEnumeration enumerateBranch(List<Operator> branch) {
        PlanEnumeration branchEnumeration = null;
        for (Operator operator : branch) {

            PlanEnumeration operatorEnumeration;
            if (operator.isAlternative()) {
                operatorEnumeration = null;
                for (OperatorAlternative.Alternative alternative : ((OperatorAlternative) operator).getAlternatives()) {
                    final PlanEnumerator alternativeEnumerator = new PlanEnumerator(alternative, this.timeEstimates, this.pruningStrategies);
                    alternativeEnumerator.run();
                    final PlanEnumeration alternativeEnumeration = alternativeEnumerator.joinAllCompleteEnumerations();
                    if (alternativeEnumeration != null) {
                        if (operatorEnumeration == null) operatorEnumeration = alternativeEnumeration;
                        else operatorEnumeration.subsume(alternativeEnumeration);
                    }
                }

            } else {
                if (!operator.isElementary()) {
                    throw new IllegalStateException("Expect elementary operator.");
                }
                if (!operator.isExecutionOperator()) {
                    this.logger.debug("Detected non-execution branch... skipping.");
                    return null;
                }
                operatorEnumeration = PlanEnumeration.createSingleton((ExecutionOperator) operator);
            }

            branchEnumeration = branchEnumeration == null
                    ? operatorEnumeration
                    : branchEnumeration.join(operatorEnumeration);
        }

        return branchEnumeration;
    }

    private boolean activateDownstreamOperators(List<Operator> branch, PlanEnumeration branchEnumeration) {
        final Operator lastBranchOperator = branch.get(branch.size() - 1);
        return Arrays.stream(lastBranchOperator.getAllOutputs())
                .flatMap(outputSlot -> outputSlot.getOccupiedSlots().stream())
                .mapToInt(adjacentInputSlot -> {
                    final Operator adjacentOperator = adjacentInputSlot.getOwner();
                    OperatorActivation adjacentOperatorActivation = this.activationCollector.get(adjacentOperator);
                    if (adjacentOperatorActivation == null) {
                        adjacentOperatorActivation = new OperatorActivation(adjacentOperator);
                        this.activationCollector.put(adjacentOperator, adjacentOperatorActivation);
                    }
                    adjacentOperatorActivation.register(branchEnumeration, adjacentInputSlot);
                    return 1;
                }).sum() > 0;
    }

    private void updateActivatedOperator() {
        this.activatedOperators.clear();
        this.activationCollector.values().stream()
                .filter(OperatorActivation::canBeActivated)
                .forEach(this.activatedOperators::add);
        for (OperatorActivation activatedOperator : this.activatedOperators) {
            this.activationCollector.remove(activatedOperator.getOperator());
        }
        this.activatedOperators.stream()
                .map(OperatorActivation::getOperator)
                .forEach(this.activationCollector::remove);
    }

    public PlanEnumeration getComprehensiveEnumeration() {
        final PlanEnumeration comprehensiveEnumeration = this.joinAllCompleteEnumerations();
        if (comprehensiveEnumeration == null) {
            throw new RheemException("Could not find a single execution plan.");
        }
        return comprehensiveEnumeration;
    }

    /**
     * Creates a new instance by <ol>
     * <li>escaping all terminal operations (in {@link #completedEnumerations}) from {@link #alternative} and</li>
     * <li>joining them.</li>
     * </ol>
     *
     * @return the new instance or {@code null} if none
     */
    private PlanEnumeration joinAllCompleteEnumerations() {
        return this.completedEnumerations.stream()
                .map(instance -> instance.escape(this.alternative))
                .reduce(PlanEnumeration::join)
                .orElse(null);
    }

    public void addPruningStrategy(PlanEnumerationPruningStrategy strategy) {
        this.pruningStrategies.add(strategy);
    }

    private void prune(final PlanEnumeration planEnumeration) {
        this.pruningStrategies.forEach(strategy -> strategy.prune(planEnumeration));
    }

    /**
     * An {@link Operator} can be activated as soon as all of its inputs are available. The inputs are served by
     * {@link PlanEnumeration}s.
     */
    public class OperatorActivation {

        /**
         * Should be eventually activated.
         */
        private final Operator activatableOperator;

        /**
         * Collects the {@link PlanEnumeration}s for the various inputs.
         */
        private final PlanEnumeration[] activationCollector;

        /**
         * Creates a new instance for the given {@link Operator}.
         *
         * @param activatableOperator should be eventually activated
         */
        private OperatorActivation(Operator activatableOperator) {
            this.activatableOperator = activatableOperator;
            this.activationCollector = new PlanEnumeration[this.activatableOperator.getNumInputs()];
        }

        /**
         * Tells whether all inputs of the {@link #activatableOperator} are served.
         */
        private boolean canBeActivated() {
            return Arrays.stream(this.activationCollector).allMatch(Objects::nonNull);
        }

        private void register(PlanEnumeration planEnumeration, InputSlot activatedInputSlot) {
            if (activatedInputSlot.getOwner() != this.activatableOperator) {
                throw new IllegalArgumentException("Slot does not belong to the activatable operator.");
            }
            int index = activatedInputSlot.getIndex();
            if (this.activationCollector[index] != null) {
                throw new IllegalStateException("Slot is already activated.");
            } else {
                this.activationCollector[index] = planEnumeration;
            }
        }

        public Operator getOperator() {
            return this.activatableOperator;
        }
    }

}
