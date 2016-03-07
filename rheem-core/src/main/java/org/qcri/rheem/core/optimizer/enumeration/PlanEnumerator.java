package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
     * {@link EnumerationActivator}s that are activated and should be followed to create branches.
     */
    private final Queue<EnumerationActivator> activatedEnumerations = new LinkedList<>();

    /**
     * {@link ConcatenationActivator}s that are activated and should be executed.
     */
    private final Queue<ConcatenationActivator> activatedConcatenations = new LinkedList<>();

    /**
     * TODO
     * When this instance enumerates an {@link OperatorAlternative.Alternative}, then this field helps to
     * create correct {@link PlanEnumeration}s by mapping the enumerates {@link Operator}s to the {@link OperatorAlternative}'s
     * slots.
     */
    private final OperatorAlternative.Alternative enumeratedAlternative;

    /**
     * Maintain {@link EnumerationActivator} for {@link Operator}s.
     */
    private final Map<Tuple<Operator, OptimizationContext>, EnumerationActivator> enumerationActivators = new HashMap<>();

    /**
     * Maintain {@link ConcatenationActivator}s for each {@link OutputSlot}.
     */
    private final Map<Tuple<OutputSlot<?>, OptimizationContext>, ConcatenationActivator> concatenationActivators = new HashMap<>();

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
     * {@link OperatorAlternative}s that have been settled already and must be respected during enumeration.
     */
    private final Map<OperatorAlternative, OperatorAlternative.Alternative> presettledAlternatives;

    /**
     * {@link ExecutionTask}s that have already been executed.
     */
    private final Map<ExecutionOperator, ExecutionTask> executedTasks;

    /**
     * {@link OptimizationContext} that holds all relevant task data.
     */
    private final OptimizationContext rootOptimizationContext;

    /**
     * Creates a new instance.
     *
     * @param rheemPlan a hyperplan that should be used for enumeration.
     */
    public PlanEnumerator(RheemPlan rheemPlan,
                          OptimizationContext optimizationContext,
                          Configuration configuration) {
        this(rheemPlan.collectReachableTopLevelSources(),
                optimizationContext,
                configuration,
                new LinkedList<>(),
                null,
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    /**
     * Creates a new instance, thereby encorporating already executed parts of the {@code rheemPlan}.
     *
     * @param rheemPlan a hyperplan that should be used for enumeration.
     * @param baseplan  an {@link ExecutionPlan} that has been already executed (for re-optimization)
     */
    public PlanEnumerator(RheemPlan rheemPlan,
                          OptimizationContext optimizationContext,
                          Configuration configuration,
                          ExecutionPlan baseplan) {

        this(rheemPlan.collectReachableTopLevelSources(),
                optimizationContext,
                configuration,
                new LinkedList<>(),
                null,
                new HashMap<>(),
                new HashMap<>());

        final Set<ExecutionTask> executedTasks = baseplan.collectAllTasks();
        executedTasks.forEach(task -> this.executedTasks.put(task.getOperator(), task));
        executedTasks.stream()
                .map(ExecutionTask::getOperator)
                .flatMap(this::streamPickedAlternatives)
                .forEach(alternative -> this.presettledAlternatives.put(alternative.toOperator(), alternative));
    }

    /**
     * TODO
     * Fork constructor.
     * <p>Forking happens to enumerate a certain {@link OperatorAlternative.Alternative} in a recursive manner.</p>
     */
    private PlanEnumerator(OperatorAlternative.Alternative enumeratedAlternative,
                           OptimizationContext optimizationContext,
                           Configuration configuration,
                           Collection<PlanEnumerationPruningStrategy> pruningStrategies,
                           Map<OperatorAlternative, OperatorAlternative.Alternative> presettledAlternatives,
                           Map<ExecutionOperator, ExecutionTask> executedTasks) {
        this(Operators.collectStartOperators(enumeratedAlternative),
                optimizationContext,
                configuration,
                pruningStrategies,
                enumeratedAlternative,
                presettledAlternatives,
                executedTasks
        );
    }

    /**
     * Basic constructor that will always be called and initializes all fields.
     */
    private PlanEnumerator(Collection<Operator> startOperators,
                           OptimizationContext optimizationContext,
                           Configuration configuration,
                           Collection<PlanEnumerationPruningStrategy> pruningStrategies,
                           OperatorAlternative.Alternative enumeratedAlternative,
                           Map<OperatorAlternative, OperatorAlternative.Alternative> presettledAlternatives,
                           Map<ExecutionOperator, ExecutionTask> executedTasks) {

        startOperators.stream()
                .map(operator -> new EnumerationActivator(operator, optimizationContext))
                .forEach(this.activatedEnumerations::add);
        this.rootOptimizationContext = optimizationContext;
        this.configuration = configuration;
        this.pruningStrategies = pruningStrategies;
        this.enumeratedAlternative = enumeratedAlternative;
        this.presettledAlternatives = presettledAlternatives;
        this.executedTasks = executedTasks;

        // Initialize pruning strategies.
        this.configuration.getPruningStrategiesProvider().forEach(this::addPruningStrategy);
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

    private Stream<OperatorAlternative.Alternative> streamPickedAlternatives(Operator operator) {
        final OperatorContainer container = operator.getContainer();
        if (container == null) return Stream.empty();
        OperatorAlternative.Alternative alternative = (OperatorAlternative.Alternative) container;
        final OperatorAlternative operatorAlternative = alternative.toOperator();
        return Stream.concat(Stream.of(alternative), this.streamPickedAlternatives(operatorAlternative));
    }

    /**
     * Execute the enumeration. Needs only be run once and will be called by one of the result retrieval methods.
     */
    private synchronized void run() {
        if (this.resultReference == null) {
            this.logger.debug("Enumerating with the activated operators {}.", this.activatedEnumerations.stream()
                    .map(EnumerationActivator::getOperator)
                    .collect(Collectors.toList()));

            while (!this.activatedEnumerations.isEmpty() || !this.activatedConcatenations.isEmpty()) {
                // Try to enumerate branches.
                EnumerationActivator enumerationActivator;
                while ((enumerationActivator = this.activatedEnumerations.poll()) != null) {
                    this.logger.debug("Executing enumeration for {}.", enumerationActivator.getKey().getField0());
                    this.enumerateBranchStartingFrom(enumerationActivator);
                }

                ConcatenationActivator concatenationActivator;
                while ((concatenationActivator = this.activatedConcatenations.poll()) != null) {
                    this.logger.debug("Executing concatenation for {}.", concatenationActivator.getKey().getField0());
                    this.concatenationActivators.remove(concatenationActivator.getKey());
                    this.concatenate(concatenationActivator);
                }
            }

            this.constructResultEnumeration();
        }
    }

    /**
     * Enumerate plans from the branch that starts at the given node. The mode of operation is as follows:
     * <ol>
     * <li>Enumerate all {@link Operator}s forming the branch.</li>
     * <li>Create a new {@link PlanEnumeration} for the branch.</li>
     * <li>Join the branch {@link PlanEnumeration} with all existing input {@link PlanEnumeration}s.</li>
     * <li>Activate downstream {@link Operator}s for upcoming branch enumerations.</li>
     * </ol>
     *
     * @param enumerationActivator the activated {@link EnumerationActivator}
     */
    private void enumerateBranchStartingFrom(EnumerationActivator enumerationActivator) {
        // Start with the activated operator.
        Operator currentOperator = enumerationActivator.activatableOperator;
        this.logger.trace("Creating new branch starting at {}.", currentOperator);
        List<Operator> branch = this.collectBranchOperatorsStartingFrom(currentOperator);
        if (branch == null) {
            return;
        }

        // Go over the branch and create a PlanEnumeration for it.
        final OptimizationContext currentOptimizationCtx = enumerationActivator.getOptimizationContext();
        PlanEnumeration branchEnumeration = this.enumerateBranch(branch, currentOptimizationCtx);
        if (branchEnumeration == null) {
            return;
        }
        this.prune(branchEnumeration);

        this.postProcess(branchEnumeration, currentOptimizationCtx);
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
            boolean isEnumeratable = currentOperator.isExecutionOperator() ||
                    currentOperator.isAlternative() ||
                    currentOperator.isLoopSubplan();
            if (!isEnumeratable) {
                this.logger.trace("Cannot enumerate branch with {}.", currentOperator);
                return null;
            }
            branch.add(currentOperator);
            // Try to advance. This requires certain conditions, though.
            if (currentOperator.getNumOutputs() != 1) {
                this.logger.trace("Stopping branch, because operator does not have exactly one output.");
                break;
            }
            if (currentOperator.getOutput(0).getOccupiedSlots().size() != 1) {
                this.logger.trace("Stopping branch, because operator does not feed exactly one operator.");
                break;
            }
            Operator nextOperator = currentOperator.getOutput(0).getOccupiedSlots().get(0).getOwner();
            if (nextOperator.getNumInputs() != 1) {
                this.logger.trace("Stopping branch, because next operator does not have exactly one input.");
                break;
            }

            currentOperator = nextOperator;
        }
        this.logger.trace("Determined branch : {}.", currentOperator);

        return branch;
    }

    /**
     * Create a {@link PlanEnumeration} for the given {@code branch}.
     *
     * @param branch              {@link List} of {@link Operator}s of the branch; ordered downstream
     * @param optimizationContext in which the {@code branch} resides
     * @return a {@link PlanEnumeration} for the given {@code branch}
     */
    private PlanEnumeration enumerateBranch(List<Operator> branch, OptimizationContext optimizationContext) {
        PlanEnumeration branchEnumeration = null;

        Operator lastOperator = null;
        for (Operator operator : branch) {
            PlanEnumeration operatorEnumeration;
            if (operator.isAlternative()) {
                operatorEnumeration = this.enumerateAlternative((OperatorAlternative) operator, optimizationContext);
            } else if (operator.isLoopSubplan()) {
                operatorEnumeration = this.enumerateLoop((LoopSubplan) operator, optimizationContext);
            } else {
                assert operator.isExecutionOperator();
                operatorEnumeration = PlanEnumeration.createSingleton((ExecutionOperator) operator, optimizationContext);
            }

            if (branchEnumeration == null) {
                branchEnumeration = operatorEnumeration;
            } else {
                branchEnumeration = PlanEnumeration.concatenate(branchEnumeration,
                        lastOperator.getOutput(0),
                        Collections.singletonMap(operator.getInput(0), operatorEnumeration),
                        optimizationContext);
            }

            lastOperator = operator;
        }

        return branchEnumeration;
    }

    /**
     * Create a {@link PlanEnumeration} for the given {@code operatorAlternative}.
     *
     * @param operatorAlternative {@link OperatorAlternative}s that should be enumerated
     * @param optimizationContext in which the {@code operatorAlternative} resides
     * @return a {@link PlanEnumeration} for the given {@code operatorAlternative}
     */
    private PlanEnumeration enumerateAlternative(OperatorAlternative operatorAlternative, OptimizationContext optimizationContext) {
        PlanEnumeration result = null;
        final List<OperatorAlternative.Alternative> alternatives =
                this.presettledAlternatives == null || !this.presettledAlternatives.containsKey(operatorAlternative) ?
                        operatorAlternative.getAlternatives() :
                        Collections.singletonList(this.presettledAlternatives.get(operatorAlternative));
        for (OperatorAlternative.Alternative alternative : alternatives) {

            // Recursively enumerate all alternatives.
            final PlanEnumerator alternativeEnumerator = this.forkFor(alternative, optimizationContext);
            final PlanEnumeration alternativeEnumeration = alternativeEnumerator.enumerate(false);

            if (alternativeEnumeration != null) {
                if (result == null) result = alternativeEnumeration.escape(alternative);
                else result.unionInPlace(alternativeEnumeration);
            }
        }
        return result;
    }

    /**
     * Fork a new instance to enumerate the given {@code alternative}.
     *
     * @param alternative         an {@link OperatorAlternative.Alternative} to be enumerated recursively
     * @param optimizationContext
     * @return the new instance
     */
    private PlanEnumerator forkFor(OperatorAlternative.Alternative alternative, OptimizationContext optimizationContext) {
        return new PlanEnumerator(alternative,
                optimizationContext,
                this.configuration,
                this.pruningStrategies,
                this.presettledAlternatives,
                this.executedTasks);
    }

    /**
     * Create a {@link PlanEnumeration} for the given {@code loop}.
     */
    private PlanEnumeration enumerateLoop(LoopSubplan loop, OptimizationContext operatorContext) {
        throw new RuntimeException("Cannot enumerate loops, yet.");
    }

    private void concatenate(ConcatenationActivator concatenationActivator) {
        final PlanEnumeration concatenatedEnumeration = PlanEnumeration.concatenate(
                concatenationActivator.baseEnumeration,
                concatenationActivator.outputSlot,
                concatenationActivator.getAdjacentEnumerations(),
                concatenationActivator.getOptimizationContext());

        this.postProcess(concatenatedEnumeration, concatenationActivator.optimizationContext);
    }

    /**
     * Sends activations to relevant {@link #enumerationActivators} or {@link #concatenationActivators}.
     *
     * @param processedEnumeration from that the activations should be sent
     * @param optimizationCtx      of the {@code processedEnumeration}
     */
    private void postProcess(PlanEnumeration processedEnumeration, OptimizationContext optimizationCtx) {
        if (processedEnumeration.isEarthed()) {
            this.postProcessEarthedEnumeration(processedEnumeration, optimizationCtx);
        } else {
            this.postProcessLooseEnumeration(processedEnumeration, optimizationCtx);
        }
    }

    /**
     * Activate successive enumerations and wait for concatenations or mark as complete.
     */
    private void postProcessEarthedEnumeration(PlanEnumeration earthedEnumeration, OptimizationContext optimizationCtx) {
        // If all input dependencies are satisfied...

        // Activate all successive operators for enumeration.
        int numDownstreamActivations = 0;
        for (Tuple<OutputSlot<?>, InputSlot<?>> inputService : earthedEnumeration.getServingOutputSlots()) {
            final InputSlot<?> servedInput = inputService.getField1();
            if (servedInput == null) continue;
            final Operator servedOperator = servedInput.getOwner();
            Tuple<Operator, OptimizationContext> activatorKey = EnumerationActivator.createKey(servedOperator, optimizationCtx);
            EnumerationActivator enumerationActivator = this.enumerationActivators.computeIfAbsent(
                    activatorKey, key -> new EnumerationActivator(key.getField0(), key.getField1()));
            enumerationActivator.register(earthedEnumeration, servedInput);
            this.logger.debug("Registering {} for enumeration of {}.", earthedEnumeration, activatorKey.getField0());
            if (enumerationActivator.canBeActivated()) {
                this.logger.debug("Activating enumeration for {}.", activatorKey.getField0());
                this.enumerationActivators.remove(enumerationActivator.getKey());
                this.activatedEnumerations.add(enumerationActivator);
            }
            numDownstreamActivations++;
        }

        if (numDownstreamActivations == 0) {
            // If the PlanEnumeration cannot be extended, it is complete.
            this.completedEnumerations.add(earthedEnumeration);

        } else {
            // Otherwise, wait for concatenation of the successive enumerations.
            for (Tuple<OutputSlot<?>, InputSlot<?>> outputService : earthedEnumeration.getServingOutputSlots()) {
                final InputSlot<?> input = outputService.getField1();
                if (input == null) continue;
                final OutputSlot<?> output = outputService.getField0();
                Tuple<OutputSlot<?>, OptimizationContext> activatorKey = ConcatenationActivator.createKey(output, optimizationCtx);
                final ConcatenationActivator concatenationActivator = this.concatenationActivators.computeIfAbsent(
                        activatorKey, key -> new ConcatenationActivator(key.getField0(), key.getField1()));
                concatenationActivator.updateBaseEnumeration(earthedEnumeration);
            }
        }
    }

    /**
     * Activate earthed enumerations for concatenation of the processed enumeration.
     */
    private void postProcessLooseEnumeration(PlanEnumeration looseEnumeration, OptimizationContext optimizationCtx) {
        // If there are open InputSlots, activate concatenations.
        for (InputSlot requestedInput : looseEnumeration.getRequestedInputSlots()) {
            final OutputSlot requestedOutput = requestedInput.getOccupant();
            if (requestedOutput == null) continue;
            Tuple<OutputSlot<?>, OptimizationContext> activatorKey = ConcatenationActivator.createKey(requestedOutput, optimizationCtx);
            final ConcatenationActivator activator = this.concatenationActivators.get(activatorKey);
            assert activator != null : String.format("No concatenation activator for %s (requested by %s).",
                    activatorKey, looseEnumeration);
            boolean wasActivated = activator.canBeActivated();
            activator.register(looseEnumeration, requestedInput);
            if (activator.canBeActivated()) {
                if (!wasActivated) {
                    this.logger.debug("Activating concatenation for {}.", activatorKey.getField0());
                    this.activatedConcatenations.add(activator);
                } else {
                    activator.updateBaseEnumeration(looseEnumeration);
                }
            }
        }
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
//        final PlanEnumeration resultEnumeration = this.completedEnumerations.stream()
//                .map(instance -> instance.escape(this.enumeratedAlternative))
//                .reduce(PlanEnumeration::join)
//                .orElse(null);
        final PlanEnumeration resultEnumeration = RheemCollections.getSingleOrNull(this.completedEnumerations);
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
    public static class EnumerationActivator {

        /**
         * Should be eventually activated.
         */
        private final Operator activatableOperator;

        /**
         * The {@link OptimizationContext} in that the {@link #activatableOperator} resides.
         */
        private final OptimizationContext optimizationContext;

        /**
         * Collects the {@link PlanEnumeration}s for the various inputs.
         */
        private final PlanEnumeration[] activationCollector;

        /**
         * Creates a new instance for the given {@link Operator}.
         *
         * @param activatableOperator should be eventually activated
         */
        private EnumerationActivator(Operator activatableOperator, OptimizationContext optimizationContext) {
            this.activatableOperator = activatableOperator;
            this.optimizationContext = optimizationContext;
            this.activationCollector = new PlanEnumeration[this.activatableOperator.getNumInputs()];
        }

        /**
         * Tells whether all inputs of the {@link #activatableOperator} are served.
         */
        private boolean canBeActivated() {
            return Arrays.stream(this.activationCollector).allMatch(Objects::nonNull);
        }

        private void register(PlanEnumeration planEnumeration, InputSlot activatedInputSlot) {
            assert activatedInputSlot.getOwner() == this.activatableOperator
                    : "Slot does not belong to the activatable operator.";
            int index = activatedInputSlot.getIndex();
            assert this.activationCollector[index] == null : "Slot is already activated.";
            this.activationCollector[index] = planEnumeration;
        }

        public Operator getOperator() {
            return this.activatableOperator;
        }

        public OptimizationContext getOptimizationContext() {
            return this.optimizationContext;
        }

        public Tuple<Operator, OptimizationContext> getKey() {
            return createKey(this.activatableOperator, this.optimizationContext);
        }

        public static Tuple<Operator, OptimizationContext> createKey(Operator operator, OptimizationContext optimizationContext) {
            return new Tuple<>(operator, optimizationContext);
        }
    }

    /**
     * TODO. Waiting for all {@link InputSlot}s for an {@link OutputSlot} in order to join.
     */
    public static class ConcatenationActivator {

        /**
         * Base plan that provides the {@link #outputSlot}. May change.
         */
        private PlanEnumeration baseEnumeration;

        /**
         * The {@link OptimizationContext} for the {@link #baseEnumeration}.
         */
        private final OptimizationContext optimizationContext;

        /**
         * Collects the {@link PlanEnumeration}s for various adjacent {@link InputSlot}s.
         */
        private final Map<InputSlot<?>, PlanEnumeration> activationCollector;

        /**
         * The number of required activations.
         */
        private final int numRequiredActivations;

        /**
         * The {@link OutputSlot} that should be concatenated.
         */
        private final OutputSlot<?> outputSlot;

        private ConcatenationActivator(OutputSlot<?> outputSlot, OptimizationContext optimizationContext) {
            assert !outputSlot.getOccupiedSlots().isEmpty();
            this.outputSlot = outputSlot;
            this.optimizationContext = optimizationContext;
            this.numRequiredActivations = (int) outputSlot.getOccupiedSlots().stream().filter(Objects::nonNull).count();
            this.activationCollector = new HashMap<>(this.numRequiredActivations);
        }

        private boolean canBeActivated() {
            assert this.numRequiredActivations >= this.activationCollector.size();
            return this.numRequiredActivations == this.activationCollector.size();
        }

        private void register(PlanEnumeration planEnumeration, InputSlot openInputSlot) {
            assert openInputSlot.getOccupant() == this.outputSlot;
            this.activationCollector.put(openInputSlot, planEnumeration);
            assert this.numRequiredActivations >= this.activationCollector.size();
        }

        public PlanEnumeration getBaseEnumeration() {
            return this.baseEnumeration;
        }

        public void updateBaseEnumeration(PlanEnumeration baseEnumeration) {
            if (this.baseEnumeration == null || this.baseEnumeration.getScope().stream().anyMatch(baseEnumeration.getScope()::contains)) {
                assert this.baseEnumeration == null || baseEnumeration.getScope().containsAll(this.baseEnumeration.getScope());
                this.baseEnumeration = baseEnumeration;
            }
        }

        public Map<InputSlot<?>, PlanEnumeration> getAdjacentEnumerations() {
            return this.activationCollector;
        }

        public OutputSlot<?> getOutputSlot() {
            return this.outputSlot;
        }

        public Tuple<OutputSlot<?>, OptimizationContext> getKey() {
            return createKey(this.outputSlot, this.optimizationContext);
        }

        public static Tuple<OutputSlot<?>, OptimizationContext> createKey(OutputSlot<?> outputSlot, OptimizationContext optimizationContext) {
            return new Tuple<>(outputSlot, optimizationContext);
        }

        public OptimizationContext getOptimizationContext() {
            return optimizationContext;
        }
    }

}
