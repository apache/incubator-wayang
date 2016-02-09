package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * The plan partitioner recursively dissects a {@link RheemPlan} into {@link PlanEnumeration}s and then assembles
 * them.
 */
public class PlanEnumerator {

    /**
     * Logger.
     */
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * {@link Configuration} of the {@link Job} on whose behalf that this instance operates.
     */
    private final Configuration configuration;

    /**
     * {@link PlanEnumerator.OperatorActivation}s that can be activated and should be followed to create branches.
     */
    private final List<OperatorActivation> activatedOperators = new LinkedList<>();

    /**
     * When this instance enumerates an {@link OperatorAlternative.Alternative}, then this field helps to
     * create correct {@link PlanEnumeration}s by mapping the enumerates {@link Operator}s to the {@link OperatorAlternative}'s
     * slots.
     */
    private final OperatorAlternative.Alternative enumeratedAlternative;

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
     * Once this instance has been executed (via {@link #run()}, the result will be stored in this field. Prior to that,
     * it is {@code null}.
     */
    private AtomicReference<PlanEnumeration> resultReference;

    /**
     * Creates a new instance.
     *
     * @param rheemPlan a hyperplan that should be used for enumeration.
     */
    public PlanEnumerator(RheemPlan rheemPlan, Configuration configuration) {
        this(rheemPlan.collectReachableTopLevelSources(), configuration, new LinkedList<>(), null);
    }

    /**
     * Fork constructor.
     * <p>Forking happens to enumerate a certain {@link OperatorAlternative.Alternative} in a recursive manner.</p>
     */
    private PlanEnumerator(OperatorAlternative.Alternative enumeratedAlternative,
                           Configuration configuration,
                           Collection<PlanEnumerationPruningStrategy> pruningStrategies) {
        this(findStartOperators(enumeratedAlternative), configuration, pruningStrategies, enumeratedAlternative);
    }

    /**
     * Basic constructor that will always be called and initializes all fields.
     */
    private PlanEnumerator(Collection<Operator> startOperators,
                           Configuration configuration,
                           Collection<PlanEnumerationPruningStrategy> pruningStrategies,
                           OperatorAlternative.Alternative enumeratedAlternative) {
        startOperators.stream()
                .map(PlanEnumerator.OperatorActivation::new)
                .forEach(this.activatedOperators::add);
        this.configuration = configuration;
        this.pruningStrategies = pruningStrategies;
        this.enumeratedAlternative = enumeratedAlternative;
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
     * Produce the {@link PlanEnumeration} for the plan specified during the construction of this instance.
     *
     * @param isRequireResult whether the result is allowed to be {@code null}
     * @return the result {@link PlanEnumeration} or {@code null} if none such exists
     */
    public PlanEnumeration enumerate(boolean isRequireResult) {
        this.run();
        final PlanEnumeration comprehensiveEnumeration = this.resultReference.get();
        if (isRequireResult && comprehensiveEnumeration == null) {
            throw new RheemException("Could not find a single execution plan.");
        }
        return comprehensiveEnumeration;
    }

    /**
     * Execute the enumeration. Needs only be run once and will be called by one of the result retrieval methods.
     */
    private synchronized void run() {
        if (this.resultReference == null) {
            this.logger.debug("Enumerating with the activated operators {}.", this.activatedOperators.stream()
                    .map(OperatorActivation::getOperator)
                    .collect(Collectors.toList()));

            while (!this.activatedOperators.isEmpty()) {
                // Create branches for all activated operators.
                this.activatedOperators.forEach(this::enumerateBranchStartingFrom);

                // Clean up the old activated operators and spawn the new ones.
                this.updateActivatedOperator();
            }

            this.constructResultEnumeration();
        }
    }

    /**
     * Enumerate plans from the branch that starts at the given node.
     *
     * @param operatorActivation the activated {@link PlanEnumerator.OperatorActivation}
     */
    private void enumerateBranchStartingFrom(OperatorActivation operatorActivation) {
        // Start with the activated operator.
        Operator currentOperator = operatorActivation.activatableOperator;
        this.logger.trace("Creating new branch starting at {}.", currentOperator);
        List<Operator> branch = this.collectBranchOperatorsStartingFrom(currentOperator);
        if (branch == null) {
            return;
        }

        // Go over the branch and create a PlanEnumeration for it.
        PlanEnumeration branchEnumeration = this.enumerateBranch(branch);
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
        if (!this.activateDownstreamOperators(branch, completeEnumeration)) {
            this.completedEnumerations.add(completeEnumeration);
        }
    }

    /**
     * Determine the branch (straight of operators) that begins at the given {@link Operator}.
     *
     * @param startOperator starts the branch
     * @return the {@link Operator}s of the branch in their order of appearance or {@code null} if the branch is
     * already known to not yield any enumerations
     */
    private List<Operator> collectBranchOperatorsStartingFrom(Operator startOperator) {
        List<Operator> branch = new LinkedList<>();
        Operator currentOperator = startOperator;
        while (true) {
            if (!currentOperator.isAlternative() && !currentOperator.isExecutionOperator()) {
                this.logger.debug("Detected invalid branch with {}.", currentOperator);
                return null;
            }
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

    /**
     * Create a {@link PlanEnumeration} for the given {@code branch}.
     *
     * @param branch {@link List} of {@link Operator}s of the branch; ordered downstream
     * @return a {@link PlanEnumeration} for the given {@code branch}
     */
    private PlanEnumeration enumerateBranch(List<Operator> branch) {
        PlanEnumeration branchEnumeration = null;

        for (Operator operator : branch) {
            PlanEnumeration operatorEnumeration;
            if (operator.isAlternative()) {
                operatorEnumeration = this.enumerateAlternative((OperatorAlternative) operator);
            } else {
                if (!operator.isElementary()) {
                    throw new IllegalStateException("Expect elementary operator.");
                }
                if (!operator.isExecutionOperator()) {
                    this.logger.debug("Detected non-execution branch {}... skipping.", branch);
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

    /**
     * Create a {@link PlanEnumeration} for the given {@code operatorAlternative}.
     *
     * @param operatorAlternative {@link OperatorAlternative}s that should be enumerated
     * @return a {@link PlanEnumeration} for the given {@code operatorAlternative}
     */
    private PlanEnumeration enumerateAlternative(OperatorAlternative operatorAlternative) {
        PlanEnumeration result = null;
        for (OperatorAlternative.Alternative alternative : operatorAlternative.getAlternatives()) {

            // Recursively enumerate all alternatives.
            final PlanEnumerator alternativeEnumerator = this.forkFor(alternative);
            final PlanEnumeration alternativeEnumeration = alternativeEnumerator.enumerate(false);

            if (alternativeEnumeration != null) {
                if (result == null) result = alternativeEnumeration;
                else result.unionInPlace(alternativeEnumeration);
            }
        }
        return result;
    }

    /**
     * Fork a new instance to enumerate the given {@code alternative}.
     *
     * @param alternative an {@link OperatorAlternative.Alternative} to be enumerated recursively
     * @return the new instance
     */
    private PlanEnumerator forkFor(OperatorAlternative.Alternative alternative) {
        return new PlanEnumerator(alternative, this.configuration, this.pruningStrategies);
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

    /**
     * Creates the final {@link PlanEnumeration} by <ol>
     * <li>escaping all terminal operations (in {@link #completedEnumerations}) from {@link #enumeratedAlternative} and</li>
     * <li>joining them.</li>
     * </ol>
     * The result is stored in {@link #resultReference}. Note the outcome might be {@code null} if the traversed plan
     * did not allow to construct a valid {@link PlanEnumeration}.
     */
    private void constructResultEnumeration() {
        final PlanEnumeration resultEnumeration = this.completedEnumerations.stream()
                .map(instance -> instance.escape(this.enumeratedAlternative))
                .reduce(PlanEnumeration::join)
                .orElse(null);
        this.resultReference = new AtomicReference<>(resultEnumeration);
    }


    public void addPruningStrategy(PlanEnumerationPruningStrategy strategy) {
        this.pruningStrategies.add(strategy);
    }

    private void prune(final PlanEnumeration planEnumeration) {
        this.pruningStrategies.forEach(strategy -> strategy.prune(planEnumeration, this.configuration));
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
