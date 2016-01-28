package org.qcri.rheem.core.optimizer;

import org.qcri.rheem.core.plan.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * The plan partitioner recursively dissects a {@link PhysicalPlan} into .
 */
public class PlanEnumerator {

    /**
     * Logger.
     */
    private Logger logger = LoggerFactory.getLogger(getClass());

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
     * This instance will put all complete {@link PlanEnumeration}s here.
     */
    private final Collection<PlanEnumeration> nonActivatingEnumerations = new LinkedList<>();

    /**
     * Creates a new instance.
     *
     * @param physicalPlan a hyperplan that should be used for enumeration.
     */
    public PlanEnumerator(PhysicalPlan physicalPlan) {
        this(physicalPlan.collectReachableTopLevelSources());
    }

    private PlanEnumerator(OperatorAlternative.Alternative alternative) {
        this(findStartOperators(alternative));
        this.alternative = alternative;
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
     * Creates a new instance.
     *
     * @param startOperators form the starting point for the enumeration
     */
    private PlanEnumerator(Collection<Operator> startOperators) {
        startOperators.stream()
                .map(PlanEnumerator.OperatorActivation::new)
                .forEach(this.activatedOperators::add);
    }

    /**
     * Execute the enumeration.
     */
    public void run() {
        this.logger.info("Enumerating with the activated operators {}.", this.activatedOperators.stream()
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
        logger.info("Creating new branch starting at {}.", currentOperator);

        List<Operator> branch = determineBranch(currentOperator);

//        PlanEnumeration branchEnumeration = PlanEnumeration.createFor(
//                branch.get(0),
//                branch.get(branch.size() - 1));

        // Go over the branch and PlanEnumeration for it.
        PlanEnumeration branchEnumeration = enumerateBranch(branch);
        if (branchEnumeration == null) {
            return;
        }

        // Build the complete enumeration from head to toe.
        PlanEnumeration completeEnumeration = branchEnumeration;
        for (PlanEnumeration inputEnumeration : operatorActivation.activationCollector) {
            // Inputs might not always be available, e.g., when enumerating an alternative.
            if (inputEnumeration != null) {
                completeEnumeration = inputEnumeration.join(completeEnumeration);
            }
            // TODO: pruning.
        }

        // Once we stopped, activate all successive operators.
        if (!activateDownstreamOperators(branch, completeEnumeration)) {
            this.nonActivatingEnumerations.add(completeEnumeration);
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
            this.logger.info("Adding {} to the branch.", currentOperator);

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
                this.logger.info("Stopping branch, because operator does not have exactly one output.");
                break;
            }
            if (currentOperator.getOutput(0).getOccupiedSlots().size() != 1) {
                this.logger.info("Stopping branch, because operator does not feed exactly one operator.");
                break;
            }
            Operator nextOperator = currentOperator.getOutput(0).getOccupiedSlots().get(0).getOwner();
            if (nextOperator.getNumInputs() != 1) {
                this.logger.info("Stopping branch, because next operator does not have exactly one input.");
                break;
            }

            currentOperator = nextOperator;
        }
        this.logger.info("Determined branch : {}.", currentOperator);

        return branch;
    }

    private PlanEnumeration enumerateBranch(List<Operator> branch) {
        PlanEnumeration branchEnumeration = null;
        for (Operator operator : branch) {

            PlanEnumeration operatorEnumeration;
            if (operator.isAlternative()) {
                operatorEnumeration = null;
                for (OperatorAlternative.Alternative alternative : ((OperatorAlternative) operator).getAlternatives()) {
                    final PlanEnumerator alternativeEnumerator = new PlanEnumerator(alternative);
                    alternativeEnumerator.run();
                    final PlanEnumeration alternativeEnumeration = alternativeEnumerator.getComprehensiveEnumeration();
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
                    this.logger.info("Detected non-execution branch... skipping.");
                    return null;
                }
                operatorEnumeration = PlanEnumeration.createSingleton((ExecutionOperator) operator);
            }

            branchEnumeration = branchEnumeration == null
                    ? operatorEnumeration
                    : branchEnumeration.join(operatorEnumeration);
        }

        // TODO: pruning

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
        return this.nonActivatingEnumerations.stream()
                .map(instance -> instance.escape(this.alternative))
                .reduce((instance1, instance2) -> instance1.join(instance2))
                .orElse(null);
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
