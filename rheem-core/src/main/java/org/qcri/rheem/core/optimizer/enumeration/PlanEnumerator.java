package org.qcri.rheem.core.optimizer.enumeration;

import de.hpi.isg.profiledb.store.model.TimeMeasurement;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.OptimizationUtils;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.LoopHeadOperator;
import org.qcri.rheem.core.plan.rheemplan.LoopSubplan;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OperatorAlternative;
import org.qcri.rheem.core.plan.rheemplan.OperatorContainer;
import org.qcri.rheem.core.plan.rheemplan.Operators;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.plan.rheemplan.Slot;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToDoubleFunction;
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
     * {@link EnumerationActivator}s that are activated and should be followed to create branches.
     */
    private final Queue<EnumerationActivator> activatedEnumerations = new LinkedList<>();

    /**
     * {@link ConcatenationActivator}s that are activated and should be executed.
     */
    private final Queue<ConcatenationActivator> activatedConcatenations = new PriorityQueue<>(
            Comparator.comparingDouble(activator0 -> activator0.priority)
    );

    /**
     * Determines how to calculate the priority of {@link ConcatenationActivator}s.
     */
    private final ToDoubleFunction<ConcatenationActivator> concatenationPriorityFunction;

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
     * {@link Channel}s that are existing and must be reused when re-optimizing.
     */
    private final Map<OutputSlot<?>, Collection<Channel>> openChannels;

    /**
     * {@link OptimizationContext} that holds all relevant task data.
     */
    private final OptimizationContext optimizationContext;

    /**
     * Keeps track of the execution time.
     */
    private TimeMeasurement timeMeasurement;

    /**
     * Tells whether branches should be enumerated first.
     */
    private boolean isEnumeratingBranchesFirst;

    /**
     * Creates a new instance.
     *
     * @param rheemPlan a hyperplan that should be used for enumeration.
     */
    public PlanEnumerator(RheemPlan rheemPlan,
                          OptimizationContext optimizationContext) {
        this(rheemPlan.collectReachableTopLevelSources(),
                optimizationContext,
                null,
                Collections.emptyMap(),
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
                          ExecutionPlan baseplan,
                          Set<Channel> openChannels) {

        this(rheemPlan.collectReachableTopLevelSources(),
                optimizationContext,
                null,
                new HashMap<>(),
                new HashMap<>(),
                new HashMap<>());

        // Register all the tasks that have been executed already.
        final Set<ExecutionTask> executedTasks = baseplan.collectAllTasks();
        executedTasks.forEach(task -> this.executedTasks.put(task.getOperator(), task));

        // Find out which alternatives have been settled already.
        executedTasks.stream()
                .map(ExecutionTask::getOperator)
                .flatMap(this::streamPickedAlternatives)
                .forEach(alternative -> this.presettledAlternatives.put(alternative.toOperator(), alternative));

        // Index the existing Channels by their user-specified Operator's OutputSlot.
        // Note that we must always take the outermost OutputSlots because only those will be connected if the RheemPlan is sane.
        for (Channel openChannel : openChannels) {
            final OutputSlot<?> outputSlot = OptimizationUtils.findRheemPlanOutputSlotFor(openChannel);
            if (outputSlot != null) {
                for (OutputSlot<?> outerOutput : outputSlot.getOwner().getOutermostOutputSlots(outputSlot)) {
                    final Collection<Channel> channelSet = this.openChannels.computeIfAbsent(outerOutput, k -> new HashSet<>(1));
                    channelSet.add(openChannel);
                }
            } else {
                this.logger.error("Could not find the output slot for the open channel {}.", openChannel);
            }
        }

    }

    /**
     * Basic constructor that will always be called and initializes all fields.
     */
    private PlanEnumerator(Collection<Operator> startOperators,
                           OptimizationContext optimizationContext,
                           OperatorAlternative.Alternative enumeratedAlternative,
                           Map<OperatorAlternative, OperatorAlternative.Alternative> presettledAlternatives,
                           Map<ExecutionOperator, ExecutionTask> executedTasks,
                           Map<OutputSlot<?>, Collection<Channel>> openChannels) {

        this.optimizationContext = optimizationContext;
        this.enumeratedAlternative = enumeratedAlternative;
        this.presettledAlternatives = presettledAlternatives;
        this.executedTasks = executedTasks;
        this.openChannels = openChannels;


        // Set up start Operators.
        for (Operator startOperator : startOperators) {
            this.scheduleForEnumeration(startOperator, optimizationContext);
        }

        // Configure the enumeration.
        final Configuration configuration = this.optimizationContext.getConfiguration();
        this.isEnumeratingBranchesFirst = configuration.getBooleanProperty(
                "rheem.core.optimizer.enumeration.branchesfirst", true
        );

        // Configure the concatenations.
        final String priorityFunctionName = configuration.getStringProperty(
                "rheem.core.optimizer.enumeration.concatenationprio"
        );
        ToDoubleFunction<ConcatenationActivator> concatenationPriorityFunction;
        switch (priorityFunctionName) {
            case "slots":
                concatenationPriorityFunction = ConcatenationActivator::countNumOfOpenSlots;
                break;
            case "plans":
                concatenationPriorityFunction = ConcatenationActivator::estimateNumConcatenatedPlanImplementations;
                break;
            case "plans2":
                concatenationPriorityFunction = ConcatenationActivator::estimateNumConcatenatedPlanImplementations2;
                break;
            case "random":
                // Randomly generate a priority. However, avoid re-generate priorities, because that would increase
                // of a concatenation activator being processed, the longer it is in the queue (I guess).
                concatenationPriorityFunction = activator -> {
                    if (!Double.isNaN(activator.priority)) return activator.priority;
                    return Math.random();
                };
                break;
            case "none":
                concatenationPriorityFunction = activator -> 0d;
                break;
            default:
                throw new RheemException("Unknown concatenation priority function: " + priorityFunctionName);
        }

        boolean isInvertConcatenationPriorities = configuration.getBooleanProperty(
                "rheem.core.optimizer.enumeration.invertconcatenations", false
        );
        this.concatenationPriorityFunction = isInvertConcatenationPriorities ?
                activator -> -concatenationPriorityFunction.applyAsDouble(activator) :
                concatenationPriorityFunction;


    }

    private void scheduleForEnumeration(Operator operator, OptimizationContext optimizationContext) {
        final EnumerationActivator enumerationActivator = new EnumerationActivator(operator, optimizationContext);
        if (enumerationActivator.canBeActivated()) {
            this.activatedEnumerations.add(enumerationActivator);
        }
        this.enumerationActivators.put(enumerationActivator.getKey(), enumerationActivator);
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
            this.logger.error("No comprehensive PlanEnumeration.");
            this.logger.error("Pending enumerations: {}", this.enumerationActivators.values().stream()
                    .filter(activator -> !activator.wasExecuted())
                    .collect(Collectors.toList())
            );
            this.logger.error("Pending concatenations: {}", this.concatenationActivators.values().stream()
                    .filter(activator -> !activator.wasExecuted())
                    .collect(Collectors.toList())
            );
            throw new RheemException("Could not find a single execution plan.");
        }
        return comprehensiveEnumeration;
    }

    private Stream<OperatorAlternative.Alternative> streamPickedAlternatives(Operator operator) {
        OperatorContainer container = operator.getContainer();
        while (container != null && !(container instanceof OperatorAlternative.Alternative)) {
            container = container.toOperator().getContainer();
        }
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
            while (!this.activatedEnumerations.isEmpty()) {
                // Try to enumerate branches.
                EnumerationActivator enumerationActivator;
                if ((enumerationActivator = this.activatedEnumerations.poll()) != null) {
                    if (this.isTopLevel()) {
                        this.logger.debug("Execute {}.", enumerationActivator);
                    }
                    this.enumerateBranchStartingFrom(enumerationActivator);
                }
            }

            ConcatenationActivator concatenationActivator;
            while ((concatenationActivator = this.activatedConcatenations.poll()) != null) {
                if (this.isTopLevel()) {
                    this.logger.debug("Execute {} (open inputs: {}).",
                            concatenationActivator,
                            concatenationActivator.getBaseEnumeration().getRequestedInputSlots()
                    );
                }
                this.concatenate(concatenationActivator);
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
        assert !enumerationActivator.wasExecuted();
        enumerationActivator.markAsExecuted();

        // Start with the activated operator.
        Operator currentOperator = enumerationActivator.activatableOperator;
        List<Operator> branch = this.collectBranchOperatorsStartingFrom(currentOperator);
        if (branch == null) {
            return;
        }
        if (this.isTopLevel()) {
            this.logger.debug("Enumerating top-level {}.", branch);
        }

        // Go over the branch and create a PlanEnumeration for it.
        final OptimizationContext currentOptimizationCtx = enumerationActivator.getOptimizationContext();
        PlanEnumeration branchEnumeration = this.enumerateBranch(branch, currentOptimizationCtx);
        if (branchEnumeration == null) {
            return;
        }

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

            // Cut branches if requested.
            if (!this.isEnumeratingBranchesFirst) break;

            // Try to advance. This requires certain conditions, though.
            OutputSlot<?> followableOutput;
            if (currentOperator.isLoopHead()) {
                LoopHeadOperator loopHeadOperator = (LoopHeadOperator) currentOperator;
                if (loopHeadOperator.getLoopBodyOutputs().size() != 1) {
                    break;
                }
                followableOutput = RheemCollections.getSingle(loopHeadOperator.getLoopBodyOutputs());

            } else {
                if (currentOperator.getNumOutputs() != 1) {
                    break;
                }
                followableOutput = currentOperator.getOutput(0);
            }

            if (followableOutput.getOccupiedSlots().size() != 1) {
                this.logger.trace("Stopping branch, because operator does not feed exactly one operator.");
                break;
            }
            Operator nextOperator = currentOperator.getOutput(0).getOccupiedSlots().get(0).getOwner();
            if (nextOperator.getNumInputs() != 1) {
                this.logger.trace("Stopping branch, because next operator does not have exactly one input.");
                break;
            }

            if (nextOperator == startOperator) {
                break;
            }

            currentOperator = nextOperator;
        }
        this.logger.trace("Determined branch: {}.", currentOperator);

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
                if (operatorEnumeration == null || operatorEnumeration.getPlanImplementations().isEmpty()) {
                    this.logger.warn("No implementations enumerated for {}.", operator);
                    return null;
                }
            } else if (operator.isLoopSubplan()) {
                operatorEnumeration = this.enumerateLoop((LoopSubplan) operator, optimizationContext);
            } else {
                assert operator.isExecutionOperator();
                operatorEnumeration = PlanEnumeration.createSingleton((ExecutionOperator) operator, optimizationContext);

                // Check if the operator is filtered.
                // However, we must not filter operators that are pre-settled (i.e., that have been executed already).
                boolean isPresettled = false;
                OperatorContainer container = operator.getContainer();
                if (container instanceof OperatorAlternative.Alternative) {
                    OperatorAlternative.Alternative alternative = (OperatorAlternative.Alternative) container;
                    OperatorAlternative operatorAlternative = alternative.getOperatorAlternative();
                    isPresettled = this.presettledAlternatives.get(operatorAlternative) == alternative;
                }
                if (!isPresettled) {
                    OptimizationContext.OperatorContext operatorContext = optimizationContext.getOperatorContext(operator);
                    if (operatorContext != null && ((ExecutionOperator) operator).isFiltered(operatorContext)) {
                        this.logger.info("Filtered {} with context {}.", operator, operatorContext);
                        operatorEnumeration.getPlanImplementations().clear();
                    }
                }
            }

            if (operatorEnumeration.getPlanImplementations().isEmpty()) {
                if (this.isTopLevel()) {
                    throw new RheemException(String.format("No implementations enumerated for %s.", operator));
                } else {
                    this.logger.warn("No implementations enumerated for {}.", operator);
                }
            }

            if (branchEnumeration == null) {
                branchEnumeration = operatorEnumeration;
            } else {
                final OutputSlot<?> output = lastOperator.getOutput(0);
                branchEnumeration = branchEnumeration.concatenate(
                        output,
                        this.openChannels.get(output),
                        Collections.singletonMap(operator.getInput(0), operatorEnumeration),
                        optimizationContext,
                        this.timeMeasurement);

                if (branchEnumeration.getPlanImplementations().isEmpty()) {
                    if (this.isTopLevel()) {
                        throw new RheemException(String.format("Could not concatenate %s to %s.", lastOperator, operator));
                    } else {
                        this.logger.warn("Could not concatenate {} to {}.", lastOperator, operator);
                    }
                }
                this.prune(branchEnumeration);
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
                final PlanEnumeration escapedEnumeration = alternativeEnumeration.escape(alternative);
                if (result == null) result = escapedEnumeration;
                else result.unionInPlace(escapedEnumeration);
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
        final PlanEnumerator fork = new PlanEnumerator(Operators.collectStartOperators(alternative),
                optimizationContext,
                alternative,
                this.presettledAlternatives,
                this.executedTasks,
                this.openChannels);
        fork.setTimeMeasurement(this.timeMeasurement);
        return fork;
    }

    /**
     * Fork a new instance for the {@code optimizationContext}.
     */
    PlanEnumerator forkFor(LoopHeadOperator loopHeadOperator, OptimizationContext optimizationContext) {
        final PlanEnumerator fork = new PlanEnumerator(Operators.collectStartOperators(loopHeadOperator.getContainer()),
                optimizationContext,
                null,
                this.presettledAlternatives,
                this.executedTasks,
                this.openChannels);
        fork.setTimeMeasurement(this.timeMeasurement);
        return fork;
    }

    /**
     * Create a {@link PlanEnumeration} for the given {@code loop}.
     */
    private PlanEnumeration enumerateLoop(LoopSubplan loop, OptimizationContext operatorContext) {
        final LoopEnumerator loopEnumerator = new LoopEnumerator(this, operatorContext.getNestedLoopContext(loop));
        return loopEnumerator.enumerate();
    }

    private void concatenate(ConcatenationActivator concatenationActivator) {
        assert !concatenationActivator.wasExecuted();
        concatenationActivator.markAsExecuted();

        final PlanEnumeration concatenatedEnumeration = concatenationActivator.baseEnumeration.concatenate(
                concatenationActivator.outputSlot,
                this.openChannels.get(concatenationActivator.outputSlot),
                concatenationActivator.getAdjacentEnumerations(),
                concatenationActivator.getOptimizationContext(),
                this.timeMeasurement
        );

        if (concatenatedEnumeration.getPlanImplementations().isEmpty()) {
            this.logger.warn("No implementations enumerated after concatenating {}.", concatenationActivator.outputSlot);
            if (this.isTopLevel()) {
                throw new RheemException(String.format("No implementations that concatenate %s with %s.",
                        concatenationActivator.outputSlot,
                        concatenationActivator.outputSlot.getOccupiedSlots()
                ));
            }
        }

        this.prune(concatenatedEnumeration);

        this.postProcess(concatenatedEnumeration, concatenationActivator.optimizationContext);
    }

    /**
     * Sends activations to relevant {@link #enumerationActivators} or {@link #concatenationActivators}.
     *
     * @param processedEnumeration from that the activations should be sent
     * @param optimizationCtx      of the {@code processedEnumeration}
     */
    private void postProcess(PlanEnumeration processedEnumeration, OptimizationContext optimizationCtx) {
        if (this.deemsComprehensive(processedEnumeration)) {
            this.completedEnumerations.add(processedEnumeration);
        } else {
            this.activateUpstream(processedEnumeration, optimizationCtx);
            this.activateDownstream(processedEnumeration, optimizationCtx);
        }
    }


    /**
     * @return whether the {@code enumeration} cannot be expanded anymore (i.e., all {@link PlanEnumeration#getServingOutputSlots()} and
     * {@link PlanEnumeration#getRequestedInputSlots()} are not connected to an adjacent {@link Slot})
     */
    public boolean deemsComprehensive(PlanEnumeration enumeration) {
        return enumeration.getServingOutputSlots().stream().allMatch(
                outputService -> !deemsRelevant(outputService.getField1())
        ) && enumeration.getRequestedInputSlots().stream().allMatch(
                input -> !deemsRelevant(input)
        );
    }

    /**
     * @return whether the {@code input} is relevant and needed for the comprehensiveness of this instance
     * (if it is not {@code null} and does not feed a {@link LoopHeadOperator})
     */
    private static boolean deemsRelevant(InputSlot<?> input) {
        return input != null
                && input.getOccupant() != null
                && !input.isFeedback();
    }


    /**
     * Perform downstream activations for the {@code processedEnumeration}. This means activating downstream
     * {@link EnumerationActivator}s and updating the {@link ConcatenationActivator}s for its {@link OutputSlot}s.
     *
     * @return the number of activated {@link EnumerationActivator}s.
     */
    private int activateDownstream(PlanEnumeration processedEnumeration, OptimizationContext optimizationCtx) {
        // Activate all successive operators for enumeration.
        int numDownstreamActivations = 0;
        for (Tuple<OutputSlot<?>, InputSlot<?>> inputService : processedEnumeration.getServingOutputSlots()) {
            final OutputSlot<?> output = inputService.getField0();
            final InputSlot<?> servedInput = inputService.getField1();
            if (!deemsRelevant(servedInput)) continue;

            // Activate downstream EnumerationActivators.
            if (this.activateDownstreamEnumeration(servedInput, processedEnumeration, optimizationCtx)) {
                numDownstreamActivations++;
            }

            // Update the ConcatenationActivator for this OutputSlot.
            if (servedInput != null) {
                final ConcatenationActivator concatenationActivator = this.getOrCreateConcatenationActivator(output, optimizationCtx);
                concatenationActivator.updateBaseEnumeration(processedEnumeration);
            }
        }

        return numDownstreamActivations;
    }

    /**
     * Activates {@link EnumerationActivator}s that are downstream of the {@code processedEnumeration} via
     * {@code input}.
     *
     * @return whether an activation took place
     */
    private boolean activateDownstreamEnumeration(InputSlot<?> input,
                                                  PlanEnumeration processedEnumeration,
                                                  OptimizationContext optimizationCtx) {

        // Find or create the appropriate EnumerationActivator.
        assert input != null;
        final Operator servedOperator = input.getOwner();
        Tuple<Operator, OptimizationContext> enumerationKey = EnumerationActivator.createKey(servedOperator, optimizationCtx);
        EnumerationActivator enumerationActivator = this.enumerationActivators.computeIfAbsent(
                enumerationKey, key -> new EnumerationActivator(key.getField0(), key.getField1())
        );

        // Register if necessary.
        if (enumerationActivator.wasExecuted()) return false;
        boolean wasActivated = enumerationActivator.canBeActivated();
        enumerationActivator.register(processedEnumeration, input);

        // Try to activate.
        if (this.isTopLevel()) {
            this.logger.trace("Registering {} for enumeration of {}.", processedEnumeration, enumerationKey.getField0());
        }
        if (!wasActivated && enumerationActivator.canBeActivated()) {
            if (this.isTopLevel()) {
                this.logger.debug("Activate {}.", enumerationActivator);
            }
            this.activatedEnumerations.add(enumerationActivator);
        }

        return true;
    }

    /**
     * Activate earthed enumerations for concatenation of the processed enumeration.
     */
    private void activateUpstream(PlanEnumeration enumeration, OptimizationContext optimizationCtx) {
        // If there are open InputSlots, activate concatenations.
        for (InputSlot requestedInput : enumeration.getRequestedInputSlots()) {
            if (!deemsRelevant(requestedInput)) continue;
            this.activateUpstreamConcatenation(requestedInput, enumeration, optimizationCtx);
        }
    }

    /**
     * Activates {@link ConcatenationActivator}s that are upstream of the {@code processedEnumeration} via
     * {@code input}.
     */
    private void activateUpstreamConcatenation(InputSlot<?> input,
                                               PlanEnumeration processedEnumeration,
                                               OptimizationContext optimizationCtx) {

        // Find the OutputSlot to connect with.
        final OutputSlot output = input.getOccupant();
        assert output != null;

        // Find or create the ConcatenationActivator.
        final ConcatenationActivator concatenationActivator = this.getOrCreateConcatenationActivator(output, optimizationCtx);
        if (concatenationActivator.wasExecuted()) return;

        // Activate the ConcatenationActivator.
        boolean wasActivated = concatenationActivator.canBeActivated();
        concatenationActivator.register(processedEnumeration, input);
        if (concatenationActivator.canBeActivated()) {
            if (!wasActivated) {
                if (this.isTopLevel()) {
                    this.logger.debug("Activate {} (open inputs: {}).",
                            concatenationActivator,
                            concatenationActivator.getBaseEnumeration().getRequestedInputSlots());
                }
                this.activatedConcatenations.add(concatenationActivator);
            }
        }
    }

    private ConcatenationActivator getOrCreateConcatenationActivator(OutputSlot<?> output,
                                                                     OptimizationContext optimizationCtx) {
        Tuple<OutputSlot<?>, OptimizationContext> concatKey = createConcatenationKey(output, optimizationCtx);
        return this.concatenationActivators.computeIfAbsent(
                concatKey, key -> new ConcatenationActivator(key.getField0(), key.getField1()));
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
        final PlanEnumeration resultEnumeration = RheemCollections.getSingleOrNull(this.completedEnumerations);
        this.resultReference = new AtomicReference<>(resultEnumeration);
    }

    /**
     * Apply all the {@link PlanEnumerationPruningStrategy}s as defined by the {@link #optimizationContext}.
     *
     * @param planEnumeration to which the pruning should be applied
     */
    private void prune(final PlanEnumeration planEnumeration) {
        TimeMeasurement pruneMeasurement =
                this.timeMeasurement == null ? null : this.timeMeasurement.start("Prune");


        if (this.logger.isDebugEnabled()) {
            this.logger.debug("{} implementations for scope {}.", planEnumeration.getPlanImplementations().size(), planEnumeration.getScope());
            for (PlanImplementation planImplementation : planEnumeration.getPlanImplementations()) {
                this.logger.debug("{}: {}", planImplementation.getTimeEstimate(), planImplementation.getOperators());
            }
        }

        int numPlanImplementations = planEnumeration.getPlanImplementations().size();
        this.optimizationContext.getPruningStrategies().forEach(strategy -> strategy.prune(planEnumeration));
        this.logger.debug("Pruned plan enumeration from {} to {} implementations.",
                numPlanImplementations,
                planEnumeration.getPlanImplementations().size()
        );

        if (pruneMeasurement != null) pruneMeasurement.stop();
    }

    /**
     * Checks whether this instance is enumerating a top-level plan and is not a recursively invoked enumeration.
     *
     * @return
     */
    public boolean isTopLevel() {
        return this.optimizationContext.getParent() == null && this.enumeratedAlternative == null;
    }

    public Configuration getConfiguration() {
        return this.optimizationContext.getConfiguration();
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

        protected boolean wasExecuted = false;

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
            for (int inputIndex = 0; inputIndex < this.activationCollector.length; inputIndex++) {
                if (this.requiresActivation(inputIndex) && this.activationCollector[inputIndex] == null) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Tells whether the {@link InputSlot} at {@code inputIndex} must be explicitly activated in the
         * {@link #activationCollector}.
         */
        private boolean requiresActivation(int inputIndex) {
            final InputSlot<?> input = this.activatableOperator.getInput(inputIndex);
            return deemsRelevant(input);
        }

        private void register(PlanEnumeration planEnumeration, InputSlot activatedInputSlot) {
            assert activatedInputSlot.getOwner() == this.activatableOperator
                    : "Slot does not belong to the activatable operator.";
            int index = activatedInputSlot.getIndex();
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

        @Override
        public String toString() {
            return String.format("%s[%s, %d/%d]", this.getClass().getSimpleName(),
                    this.activatableOperator,
                    Arrays.stream(this.activationCollector).filter(Objects::nonNull).count(),
                    this.activationCollector.length);
        }

        public static Tuple<Operator, OptimizationContext> createKey(Operator operator, OptimizationContext optimizationContext) {
            return new Tuple<>(operator, optimizationContext);
        }

        protected boolean wasExecuted() {
            return this.wasExecuted;
        }

        protected void markAsExecuted() {
            this.wasExecuted = true;
        }

    }

    /**
     * TODO. Waiting for all {@link InputSlot}s for an {@link OutputSlot} in order to join.
     */
    public class ConcatenationActivator {

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

        protected boolean wasExecuted = false;

        /**
         * The priority of this instance.
         */
        private double priority = Double.NaN;


        private ConcatenationActivator(OutputSlot<?> outputSlot, OptimizationContext optimizationContext) {
            assert !outputSlot.getOccupiedSlots().isEmpty();
            this.outputSlot = outputSlot;
            this.optimizationContext = optimizationContext;
            this.numRequiredActivations = (int) outputSlot.getOccupiedSlots().stream().filter(PlanEnumerator::deemsRelevant).count();
            this.activationCollector = new HashMap<>(this.numRequiredActivations);
        }

        private boolean canBeActivated() {
            assert this.numRequiredActivations >= this.activationCollector.size();
            return this.baseEnumeration != null && this.numRequiredActivations == this.activationCollector.size();
        }

        private void register(PlanEnumeration planEnumeration, InputSlot openInputSlot) {
            assert deemsRelevant(openInputSlot)
                    : String.format("Trying to registerChannelConversion irrelevant %s to %s.", openInputSlot, this);
            assert openInputSlot.getOccupant() == this.outputSlot;
            this.activationCollector.put(openInputSlot, planEnumeration);
            assert this.numRequiredActivations >= this.activationCollector.size();

            this.updatePriority();
        }

        public PlanEnumeration getBaseEnumeration() {
            return this.baseEnumeration;
        }

        public void updateBaseEnumeration(PlanEnumeration baseEnumeration) {
            // TODO: if (this.baseEnumeration == null || this.baseEnumeration.getScope().stream().anyMatch(baseEnumeration.getScope()::contains)) {
            assert this.baseEnumeration == null || baseEnumeration.getScope().containsAll(this.baseEnumeration.getScope());
            this.baseEnumeration = baseEnumeration;
            // }

            this.updatePriority();
        }

        /**
         * Update the {@link #priority} of this instance.
         */
        private void updatePriority() {
            // If this instance is not ready for activation, it does not have or need a priority.
            if (!this.canBeActivated()) return;

            // Calculate the new priority.
            double oldPriority = this.priority;
            this.priority = PlanEnumerator.this.concatenationPriorityFunction.applyAsDouble(this);

            // Update the priority queue if needed.
            if (this.priority != oldPriority && PlanEnumerator.this.activatedConcatenations.remove(this)) {
                PlanEnumerator.this.activatedConcatenations.add(this);
            }
        }

        /**
         * Estimates the number of {@link PlanImplementation}s in the concatenated {@link PlanEnumeration}. Can be used
         * as {@link #concatenationPriorityFunction}.
         *
         * @return the number of {@link PlanImplementation}s
         */
        private double estimateNumConcatenatedPlanImplementations() {
            // We use the product of all concatenatable PlanImplementations as an estimate of the size of the
            // concatenated PlanEnumeration.
            long num = this.baseEnumeration.getPlanImplementations().size();
            for (PlanEnumeration successorEnumeration : activationCollector.values()) {
                num *= successorEnumeration.getPlanImplementations().size();
            }
            return num;
        }

        /**
         * Estimates the number of {@link PlanImplementation}s in the concatenated {@link PlanEnumeration}. Can be used
         * as {@link #concatenationPriorityFunction}.
         *
         * @return the number of {@link PlanImplementation}s
         */
        private double estimateNumConcatenatedPlanImplementations2() {
            // We use the product of all concatenatable PlanImplementations as an estimate of the size of the
            // concatenated PlanEnumeration.
            return Stream.concat(Stream.of(this.baseEnumeration), activationCollector.values().stream()).distinct().count();
        }

        /**
         * Calculates the number of open {@link Slot}s in the concatenated {@link PlanEnumeration}. Can be used
         * as {@link #concatenationPriorityFunction}.
         *
         * @return the number of open {@link Slot}s
         */
        private double countNumOfOpenSlots() {
            // We use the number of open slots in the concatenated PlanEnumeration.
            Set<Slot<?>> openSlots = new HashSet<>();
            // Add all the slots from the baseEnumeration.
            openSlots.addAll(this.baseEnumeration.getRequestedInputSlots());
            for (Tuple<OutputSlot<?>, InputSlot<?>> outputInput : this.baseEnumeration.getServingOutputSlots()) {
                openSlots.add(outputInput.getField0());
            }
            // Add all the slots from the successor enumerations.
            for (PlanEnumeration successorEnumeration : this.activationCollector.values()) {
                openSlots.addAll(successorEnumeration.getRequestedInputSlots());
                for (Tuple<OutputSlot<?>, InputSlot<?>> outputInput : successorEnumeration.getServingOutputSlots()) {
                    openSlots.add(outputInput.getField0());
                }
            }
            // Remove all the slots that are being connected.
            openSlots.remove(this.outputSlot);
            openSlots.removeAll(this.activationCollector.keySet());
            return openSlots.size();
        }

        public Map<InputSlot<?>, PlanEnumeration> getAdjacentEnumerations() {
            return this.activationCollector;
        }

        public OutputSlot<?> getOutputSlot() {
            return this.outputSlot;
        }

        public Tuple<OutputSlot<?>, OptimizationContext> getKey() {
            return createConcatenationKey(this.outputSlot, this.optimizationContext);
        }


        public OptimizationContext getOptimizationContext() {
            return this.optimizationContext;
        }

        protected boolean wasExecuted() {
            return this.wasExecuted;
        }

        protected void markAsExecuted() {
            this.wasExecuted = true;
        }

        @Override
        public String toString() {
            return String.format("%s[%s: %s -> %s]", this.getClass().getSimpleName(),
                    this.outputSlot,
                    this.baseEnumeration,
                    this.activationCollector.values());
        }


    }

    public static Tuple<OutputSlot<?>, OptimizationContext> createConcatenationKey(
            OutputSlot<?> outputSlot,
            OptimizationContext optimizationContext) {
        return new Tuple<>(outputSlot, optimizationContext);
    }

    /**
     * Provide a {@link TimeMeasurement} allowing this instance to time internally.
     *
     * @param timeMeasurement the {@link TimeMeasurement}
     */
    public void setTimeMeasurement(TimeMeasurement timeMeasurement) {
        this.timeMeasurement = timeMeasurement;
    }
}
