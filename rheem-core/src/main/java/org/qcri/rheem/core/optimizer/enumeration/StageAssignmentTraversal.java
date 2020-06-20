package org.qcri.rheem.core.optimizer.enumeration;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionStageLoop;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.executionplan.PlatformExecution;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.LoopHeadOperator;
import org.qcri.rheem.core.plan.rheemplan.LoopSubplan;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.Iterators;
import org.qcri.rheem.core.util.OneTimeExecutable;
import org.qcri.rheem.core.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Builds an {@link ExecutionPlan} from a {@link ExecutionTaskFlow}.
 * <p>Specifically, subdivides the {@link ExecutionTask}s into {@link PlatformExecution}s and {@link ExecutionStage}s,
 * thereby discarding already executed {@link ExecutionTask}s.</p>
 * <p>As of now, these are recognized as producers of
 * {@link Channel}s that are copied (see {@link Channel#isCopy()}). This is because of {@link ExecutionTaskFlowCompiler} that copies {@link Channel}s
 * to different alternative {@link ExecutionPlan}s on top of existing fixed {@link ExecutionTask}s.</p>
 */
public class StageAssignmentTraversal extends OneTimeExecutable {

    private static final Logger logger = LoggerFactory.getLogger(StageAssignmentTraversal.class);

    /**
     * Should be turned into a {@link ExecutionPlan}.
     */
    private final ExecutionTaskFlow executionTaskFlow;

    /**
     * Assigns {@link ExecutionTask}s with {@link InterimStage}s.
     */
    private final Map<ExecutionTask, InterimStage> assignedInterimStages = new HashMap<>();

    /**
     * Keeps track of {@link InterimStage}s that must be executed before executing a certain {@link ExecutionTask}.
     */
    private final Map<ExecutionTask, Set<InterimStage>> requiredStages = new HashMap<>();

    /**
     * Zero or more {@link StageSplittingCriterion}s to further refine {@link ExecutionStage}s.
     */
    private final Collection<StageSplittingCriterion> splittingCriteria = new LinkedList<>();

    /**
     * All {@link InterimStage}s created by this instance.
     */
    private final Collection<InterimStage> allStages = new LinkedList<>();

    /**
     * Newly created {@link InterimStage}s that might be subject to refinement still.
     */
    private final Collection<InterimStage> newStages = new LinkedList<>();

    /**
     * Maintains {@link ExecutionStageLoop}s that are being created.
     */
    private Map<LoopSubplan, ExecutionStageLoop> stageLoops = new HashMap<>();

    /**
     * Accepts the result of this instance after execution.
     */
    private ExecutionPlan result;

    /**
     * Creates a new instance.
     *
     * @param executionTaskFlow should be converted into an {@link ExecutionPlan}
     * @param splittingCriteria to create splits beside the precedence-based splitting
     */
    private StageAssignmentTraversal(ExecutionTaskFlow executionTaskFlow,
                                     StageSplittingCriterion... splittingCriteria) {
        // Some sanity checks.
        final Set<ExecutionTask> executionTasks = executionTaskFlow.collectAllTasks();
        for (ExecutionTask executionTask : executionTasks) {
            for (int i = 0; i < executionTask.getInputChannels().length; i++) {
                Channel channel = executionTask.getInputChannels()[i];
                if (channel == null) {
                    logger.warn("{} does not have an input channel @{}.", executionTask, i);
                }
            }
            for (int i = 0; i < executionTask.getOutputChannels().length; i++) {
                Channel channel = executionTask.getOutputChannels()[i];
                if (channel == null) {
                    logger.warn("{} does not have an output channel @{}.", executionTask, i);
                }
            }
        }
        assert executionTaskFlow.isComplete();

        // Do some initialization.
        this.executionTaskFlow = executionTaskFlow;
        // TODO: The following criterion isolates LoopHeadOperators into own ExecutionStages, so as to avoid problems connected to circular dependencies. But this might not be as performant as it gets.
        this.splittingCriteria.add(StageAssignmentTraversal::isSuitableForBreakpoint);
        this.splittingCriteria.add(StageAssignmentTraversal::isLoopHeadInvolved);
        this.splittingCriteria.add(StageAssignmentTraversal::isLoopBoarder); // Loop boards need be split always.
        this.splittingCriteria.addAll(Arrays.asList(splittingCriteria));
    }

    /**
     * Convert an {@link ExecutionTaskFlow} into an {@link ExecutionPlan} by introducing {@link ExecutionStage}s.
     *
     * @param executionTaskFlow           should be converted
     * @param additionalSplittingCriteria will be employed to split {@link ExecutionStage}s that are not split necessarily
     * @return the {@link ExecutionPlan}
     */
    public static ExecutionPlan assignStages(ExecutionTaskFlow executionTaskFlow,
                                             StageSplittingCriterion... additionalSplittingCriteria) {
        final StageAssignmentTraversal instance = new StageAssignmentTraversal(executionTaskFlow, additionalSplittingCriteria);
        return instance.buildExecutionPlan();
    }

    /**
     * Tells whether the given {@link Channel} lends itself to a {@link org.qcri.rheem.core.platform.Breakpoint}. In
     * that case, we might want to split an {@link ExecutionStage} here.
     *
     * @see StageSplittingCriterion#shouldSplit(ExecutionTask, Channel, ExecutionTask)
     */
    private static boolean isSuitableForBreakpoint(ExecutionTask producer, Channel channel, ExecutionTask consumer) {
        return channel.isSuitableForBreakpoint();
    }

    /**
     * Tells whether the given {@link Channel} leaves or enters a loop.
     *
     * @see StageSplittingCriterion#shouldSplit(ExecutionTask, Channel, ExecutionTask)
     */
    private static boolean isLoopBoarder(ExecutionTask producer, Channel channel, ExecutionTask consumer) {
        final ExecutionOperator producerOperator = producer.getOperator();
        final LinkedList<LoopSubplan> producerLoopStack = producerOperator.getLoopStack();

        final ExecutionOperator consumerOperator = consumer.getOperator();
        final LinkedList<LoopSubplan> consumerLoopStack = consumerOperator.getLoopStack();

        return !producerLoopStack.equals(consumerLoopStack);
    }

    /**
     * Tells whether the given {@link Channel} connects a {@link LoopHeadOperator}.
     *
     * @see StageSplittingCriterion#shouldSplit(ExecutionTask, Channel, ExecutionTask)
     */
    private static boolean isLoopHeadInvolved(ExecutionTask producer, Channel channel, ExecutionTask consumer) {
        return producer.getOperator().isLoopHead() || consumer.getOperator().isLoopHead();
    }

    /**
     * Perform the assignment.
     *
     * @return the {@link ExecutionPlan} for the {@link ExecutionTaskFlow} specified in the constructor
     */
    private ExecutionPlan buildExecutionPlan() {
        this.tryExecute();
        return this.result;
    }

    @Override
    protected void doExecute() {
        // Create initial stages.
        this.discoverInitialStages();

        // Refine stages as much as necessary
        this.refineStages();
        if (logger.isDebugEnabled()) {
            for (InterimStage stage : this.allStages) {
                logger.debug("Final stage {}: {}", stage, stage.getTasks());
            }
        }

        // Assemble the ExecutionPlan.
        this.result = this.assembleExecutionPlan();
    }

    /**
     * Create an initial assignment for each {@link ExecutionTask}, thereby keeping track of existing
     * {@link PlatformExecution}s.
     */
    private void discoverInitialStages() {
        // ExecutionTasks which have to be assigned an InterimStage.
        final Set<ExecutionTask> relevantTasks = new HashSet<>();

        // ExecutionTasks that are staged for exploration.
        final Queue<ExecutionTask> stagedTasks = new LinkedList<>(this.executionTaskFlow.getSinkTasks());

        // Run until all ExecutionTasks have been checked.
        while (!stagedTasks.isEmpty()) {
            final ExecutionTask task = stagedTasks.poll();

            // Collect the task and make sure we have not seen it yet.
            if (!relevantTasks.add(task)) continue;

            for (Channel inputChannel : task.getInputChannels()) {
                if (!this.shouldVisitProducerOf(inputChannel)) { // Barrier.
                    // At this point, we know that we are re-optimizing because the producer was already executed and
                    // is therefore not visited.
                    final ExecutionTask producer = inputChannel.getProducer();

                    // We need to see, if we must re-use the PlatformExecution of the producer.
                    // Most important seeds are those that might need to use an existing PlatformExecution:
                    // We need to create their InterimStages immediately to reuse the PlatformExecution before
                    // ExecutionTasks are assigned to other InterimStages.
                    if (this.checkIfShouldReusePlatformExecution(producer, inputChannel, task)) {
                        this.createStageFor(task, producer.getStage().getPlatformExecution());
                    }
                } else {
                    // Otherwise, just stage.
                    assert inputChannel.getProducer() != null;
                    stagedTasks.add(inputChannel.getProducer());
                }
            }
        }

        // Now, we can assign all InterimStages with new PlatformExecutions.
        Queue<ExecutionTask> seedTasks = new LinkedList<>(relevantTasks);
        while (!seedTasks.isEmpty()) {
            final ExecutionTask task = seedTasks.poll();
            this.createStageFor(task, null);
        }

        if (logger.isDebugEnabled()) {
            this.assignedInterimStages.values().stream().distinct().forEach(
                    stage -> logger.debug("Established initial stage with {}.", stage.getTasks())
            );
        }

        // Make sure that we didn't blunder.
        assert relevantTasks.stream().allMatch(this.assignedInterimStages::containsKey);
    }

    /**
     * Determines whether the {@code task} should reuse the {@link PlatformExecution} of the {@code producer}.
     *
     * @param producer     whose {@link PlatformExecution} might be reused
     * @param inputChannel connects the {@code producer} to the {@code task}
     * @param task         might reuse the {@link PlatformExecution} of {@code producer}
     * @return whether to reuse
     */
    private boolean checkIfShouldReusePlatformExecution(ExecutionTask producer, Channel inputChannel, ExecutionTask task) {
        final Platform producerPlatform = producer.getPlatform();
        return producerPlatform.equals(task.getPlatform()) &&
                producerPlatform.isSinglePlatformExecutionPossible(producer, inputChannel, task);
    }

    /**
     * Starts building an {@link InterimStage} starting from the given {@link ExecutionTask} unless there is one already.
     * If a {@link PlatformExecution} is provided, the {@link InterimStage} will be associated with it.
     */
    private void createStageFor(ExecutionTask task, PlatformExecution platformExecution) {
        assert task.getStage() == null : String.format("%s has already stage %s.", task, task.getStage());

        // See if there is already an InterimStage.
        if (this.assignedInterimStages.containsKey(task)) {
            return;
        }

        // Create a new PlatformExecution if none.
        if (platformExecution == null) {
            Platform platform = task.getOperator().getPlatform();
            platformExecution = new PlatformExecution(platform);
        }

        // Create the InterimStage and expand it.
        InterimStage initialStage = new InterimStageImpl(platformExecution);
        this.addStage(initialStage);
        this.assignTaskAndExpand(task, initialStage);
    }

    /**
     * Adds an {@link InterimStage} byt putting it on {@link #allStages} and {@link #newStages}.
     */
    private void addStage(InterimStage stage) {
        this.newStages.add(stage);
        this.allStages.add(stage);
    }

    /**
     * Assign the given {@link ExecutionTask} to the given {@link InterimStage}s, then expands it to adjacent {@link ExecutionTask}s.
     */
    private void assignTaskAndExpand(ExecutionTask task, InterimStage interimStage) {
        this.assign(task, interimStage);
        this.expandDownstream(task, interimStage);
        this.expandUpstream(task, interimStage);
    }

    /**
     * Assign the given {@link ExecutionTask} to the given {@link InterimStage}s and set up {@link #requiredStages}
     * for it.
     */
    private void assign(ExecutionTask task, InterimStage newStage) {
        assert task.getOperator().getPlatform().equals(newStage.getPlatform());
        newStage.addTask(task);
        final InterimStage oldStage = this.assignedInterimStages.put(task, newStage);
        logger.trace("Reassigned {} from {} to {}.", task, oldStage, newStage);
    }

    /**
     * Handle the upstream neighbors of the given {@code task} by expanding the {@code expandableStage}.
     */
    private void expandDownstream(ExecutionTask task, InterimStage expandableStage) {
        for (Channel channel : task.getOutputChannels()) {
            assert channel != null : String.format("%s has null output channels.", task);
            if (channel.isExecutionBreaker()) {
                expandableStage.setOutbound(task);
            }
            for (ExecutionTask consumer : channel.getConsumers()) {
                final InterimStage assignedStage = this.assignedInterimStages.get(consumer);
                if (assignedStage == null) {
                    this.handleTaskWithoutPlatformExecution(consumer, /*channel.isExecutionBreaker() ? null : */ expandableStage);
                }
            }
        }
    }

    /**
     * Handle the upstream neighbors of the given {@code task} by expanding the {@code expandableStage} if possible.
     */
    private void expandUpstream(ExecutionTask task, InterimStage expandableStage) {
        for (Channel channel : task.getInputChannels()) {
            if (!this.shouldVisitProducerOf(channel)) continue;
            final ExecutionTask producer = channel.getProducer();
            assert producer != null;
            final InterimStage assignedStage = this.assignedInterimStages.get(producer);
            if (assignedStage == null) {
                this.handleTaskWithoutPlatformExecution(producer, /*channel.isExecutionBreaker() ? null : */expandableStage);
            }
        }
    }

    /**
     * Handle a given {@code task} by expanding the {@code expandableStage} if possible.
     */
    private void handleTaskWithoutPlatformExecution(ExecutionTask task, InterimStage expandableStage) {
        final Platform operatorPlatform = task.getOperator().getPlatform();
        if (expandableStage != null && operatorPlatform.equals(expandableStage.getPlatform())) {
            this.assignTaskAndExpand(task, expandableStage);
        }
    }

    /**
     * Refine the current {@link #newStages} and put the result to {@link #allStages}.
     */
    private void refineStages() {
        // Apply the #splittingCriteria at first.
        new ArrayList<>(this.newStages).forEach(this::applySplittingCriteria);

        this.splitStagesByPrecedence();
    }

    /**
     * Applies all {@link #splittingCriteria} to the given {@code stage}, thereby refining it.
     */
    private void applySplittingCriteria(InterimStage stage) {
        // TODO: This splitting mechanism can cause unnecessary fragmentation of stages. Most likely, because "willTaskBeSeparated" depends on the traversal order of the stage DAG.

        // Keeps track of ExecutionTasks that should be separated from those that are not in this Set.
        Set<ExecutionTask> tasksToSeparate = new HashSet<>();

        // Maintains ExecutionTasks whose outgoing Channels have been visited.
        Set<ExecutionTask> seenTasks = new HashSet<>();

        // Maintains ExecutionTasks to be visited and checked for split criteria.
        Queue<ExecutionTask> taskQueue = new LinkedList<>(stage.getStartTasks());
        while (!taskQueue.isEmpty()) {
            final ExecutionTask task = taskQueue.poll();

            // Avoid visiting the task twice.
            if (seenTasks.add(task)) {

                // Check if the task is already marked for separation.
                boolean willTaskBeSeparated = tasksToSeparate.contains(task);

                // Visit all successor tasks and check whether they should be separated.
                for (Channel channel : task.getOutputChannels()) {
                    for (ExecutionTask consumerTask : channel.getConsumers()) {
                        // If the consumerTask is in other stage, there is no need to split.
                        if (this.assignedInterimStages.get(consumerTask) != stage) {
                            continue;
                        }

                        if (willTaskBeSeparated || this.splittingCriteria.stream().anyMatch(
                                criterion -> criterion.shouldSplit(task, channel, consumerTask)
                        )) {
                            if (consumerTask.isFeedbackInput(channel)) {
                                // TODO: Use marks to implement same-stage splits.
                                // channel.setStageExecutionBarrier(true);
                                continue;
                            }
                            tasksToSeparate.add(consumerTask);
                        }
                        taskQueue.add(consumerTask);
                    }
                }
            }
        }

        if (!tasksToSeparate.isEmpty()) {
            assert tasksToSeparate.size() < stage.getTasks().size() : String.format(
                    "Cannot separate all tasks from stage with tasks %s.", tasksToSeparate
            );
            // Prepare to split the ExecutionTasks that are not separated.
            final HashSet<ExecutionTask> tasksToKeep = new HashSet<>(stage.getTasks());
            tasksToKeep.removeAll(tasksToSeparate);

            // Separate the ExecutionTasks and create stages for each connected component.
            do {
                Set<ExecutionTask> component = this.separateConnectedComponent(tasksToSeparate);
                final InterimStage separatedStage = this.splitStage(stage, component);
                this.applySplittingCriteria(separatedStage);
            } while (!tasksToSeparate.isEmpty());

            // Also split the remainder into connected components.
            while (true) {
                Set<ExecutionTask> component = this.separateConnectedComponent(tasksToKeep);
                // Avoid "splitting" if the tasksToKeep are already a connected component.
                if (tasksToKeep.isEmpty()) break;
                final InterimStage separatedStage = this.splitStage(stage, component);
                this.applySplittingCriteria(separatedStage);
            }
        }
    }

    /**
     * Removes a connected component of {@link ExecutionTask}s.
     *
     * @param tasks from that a connected component should be removed
     * @return the connected component
     */
    private Set<ExecutionTask> separateConnectedComponent(Set<ExecutionTask> tasks) {
        assert !tasks.isEmpty();

        // Prepare data structures.
        Queue<ExecutionTask> stagedTasks = new LinkedList<>();
        Set<ExecutionTask> connectedComponent = new HashSet<>(tasks.size());

        // Remove any element from the tasks.
        final Iterator<ExecutionTask> iterator = tasks.iterator();
        final ExecutionTask seed = iterator.next();
        stagedTasks.add(seed);
        iterator.remove();

        // Expand the connected component.
        ExecutionTask task;
        while ((task = stagedTasks.poll()) != null) {
            connectedComponent.add(task);
            for (Channel channel : task.getInputChannels()) {
                if (task.isFeedbackInput(channel)) continue;
                final ExecutionTask producer = channel.getProducer();
                if (tasks.remove(producer)) {
                    stagedTasks.add(producer);
                }
            }
            for (Channel channel : task.getOutputChannels()) {
                for (ExecutionTask consumer : channel.getConsumers()) {
                    if (!consumer.isFeedbackInput(channel) && tasks.remove(consumer)) {
                        stagedTasks.add(consumer);
                    }
                }
            }
        }

        // Return the connected component.
        return connectedComponent;
    }

    /**
     * Spans a precedence graph between {@link #newStages} and splits them where necessary.
     */
    private void splitStagesByPrecedence() {
        // Assign the required stages for each ExecutionTask: Each one requires its very own stage.
        for (InterimStage stage : this.newStages) {
            for (ExecutionTask task : stage.getTasks()) {
                this.requiredStages.computeIfAbsent(task, key -> new HashSet<>(4)).add(stage);
            }
        }

        // Update the precedence graph and split until we reach a stable state.
        while (!this.newStages.isEmpty()) {

            // Update the precedence graph.
            for (InterimStage currentStage : this.newStages) {

                // We start from the outbound ExecutionTasks of each stage, because within each stage we will not create new precedences.
                for (ExecutionTask outboundTask : currentStage.getOutboundTasks()) {

                    // Start with the currently required stages.
                    final HashSet<InterimStage> requiredStages = new HashSet<>(this.requiredStages.get(outboundTask));

                    // Propagate these stages to all follow-up tasks.
                    for (Channel channel : outboundTask.getOutputChannels()) {
                        for (ExecutionTask consumer : channel.getConsumers()) {
                            if (!consumer.isFeedbackInput(channel)) {
                                this.updateRequiredStages(consumer, requiredStages);
                            }
                        }
                    }
                }
            }

            // Partition stages. Might yield new #newStages.
            this.newStages.clear();
            new ArrayList<>(this.allStages).forEach(this::partitionStage);
        }

    }

    /**
     * Update the {@link #requiredStages} while recursively traversing downstream over {@link ExecutionTask}s.
     * Also, mark its {@link InterimStage} ({@link InterimStage#markDependenciesUpdated()})
     * to keep track of dependencies between the {@link InterimStage}s that we have been unaware of so far.
     *
     * @param task           which is to be traversed next
     * @param requiredStages the required {@link InterimStage}s
     */
    private void updateRequiredStages(ExecutionTask task,
                                      Set<InterimStage> requiredStages) {
        // Find the InterimStage assigned to the task.
        final InterimStage currentStage = this.assignedInterimStages.get(task);

        // Update the requiredStages by the InterimStage of the task.
        boolean isCurrentStageAdded = requiredStages.add(currentStage);

        // Try to update the #requiredStages of our task.
        final Set<InterimStage> currentlyRequiredStages = this.requiredStages.get(task);
        if (currentlyRequiredStages.addAll(requiredStages)) {
            // If there is a new required stage, mark the stage.
            logger.debug("Updated required stages of {} to {}.", task, currentlyRequiredStages);
            currentStage.markDependenciesUpdated();

            // And propagate the dependencies downstream. We assume all downstream dependencies to be supersets of
            // the current requiredStage, which should be true by construction.
            for (Channel channel : task.getOutputChannels()) {
                for (ExecutionTask consumingTask : channel.getConsumers()) {
                    if (!consumingTask.isFeedbackInput(channel)) {
                        this.updateRequiredStages(consumingTask, requiredStages);
                    }
                }
            }
        }

        if (isCurrentStageAdded) {
            requiredStages.remove(currentStage);
        }
    }


    /**
     * Partition the {@code stage} into two halves. All {@link ExecutionTask}s that do not have the minimum count of
     * required {@link InterimStage}s will be put into a new {@link InterimStage}.
     *
     * @return whether a split occurred
     */
    private boolean partitionStage(InterimStage stage) {
        // Short-cut: if the stage has not been marked, its required stages did not change.
        if (!stage.getAndResetSplitMark()) {
            return false;
        }
        int minRequiredStages = -1;
        final Collection<ExecutionTask> initialTasks = new LinkedList<>();
        final Set<ExecutionTask> tasksToSeparate = new HashSet<>();
        for (ExecutionTask task : stage.getTasks()) {
            final Set<InterimStage> requiredStages = this.requiredStages.get(task);
            if (minRequiredStages == -1 || requiredStages.size() < minRequiredStages) {
                tasksToSeparate.addAll(initialTasks);
                initialTasks.clear();
                minRequiredStages = requiredStages.size();
            }
            (minRequiredStages == requiredStages.size() ? initialTasks : tasksToSeparate).add(task);
        }

        if (tasksToSeparate.isEmpty()) {
            logger.debug("No separable tasks found in marked stage {}.", stage);
            return false;
        } else {
            // Prepare to split the ExecutionTasks that are not separated.
            final HashSet<ExecutionTask> tasksToKeep = new HashSet<>(stage.getTasks());
            tasksToKeep.removeAll(tasksToSeparate);

            // Separate the ExecutionTasks and create stages for each connected component.
            do {
                Set<ExecutionTask> component = this.separateConnectedComponent(tasksToSeparate);
                this.splitStage(stage, component);
            } while (!tasksToSeparate.isEmpty());

            // Also split the remainder into connected components.
            while (true) {
                Set<ExecutionTask> component = this.separateConnectedComponent(tasksToKeep);
                // Avoid "splitting" if the tasksToKeep are already a connected component.
                if (tasksToKeep.isEmpty()) break;
                this.splitStage(stage, component);
            }
            return true;
        }
    }

    /**
     * Splits the given {@link ExecutionTask}s from the {@link InterimStage} to form a new {@link InterimStage},
     * which will be added to {@link #newStages}. Also. {@link #assignedInterimStages} will be updated.
     *
     * @return the new {@link InterimStage}
     */
    private InterimStage splitStage(InterimStage stage, Set<ExecutionTask> separableTasks) {
        if (logger.isDebugEnabled()) {
            Set<ExecutionTask> residualTasks = new HashSet<>(stage.getTasks());
            residualTasks.removeAll(separableTasks);
            logger.debug("Separating " + separableTasks + " from " + residualTasks + "...");

        }
        InterimStage newStage = stage.separate(separableTasks);
        this.addStage(newStage);
        for (ExecutionTask separatedTask : newStage.getTasks()) {
            this.assign(separatedTask, newStage);
        }
        return newStage;
    }

    private ExecutionPlan assembleExecutionPlan() {
        final Map<InterimStage, ExecutionStage> finalStages = new HashMap<>(this.allStages.size());
        for (ExecutionTask sinkTask : this.executionTaskFlow.getSinkTasks()) {
            this.assembleExecutionPlan(finalStages, null, sinkTask, new HashSet<>());
        }
        final ExecutionPlan executionPlan = new ExecutionPlan();
        finalStages.values().stream().filter(ExecutionStage::isStartingStage).forEach(executionPlan::addStartingStage);
        return executionPlan;
    }

    /**
     * Creates {@link ExecutionStage}s and connects them.
     *
     * @param finalStages             collects the {@link ExecutionStage}s
     * @param successorExecutionStage the {@link ExecutionStage} following the {@code currentExecutionTask}
     * @param currentExecutionTask    an {@link ExecutionTask} whose {@link InterimStage} is to be considered
     * @param visitedTasks            maintains already visited {@link ExecutionTask}s to avoid running into loops
     */
    private void assembleExecutionPlan(Map<InterimStage, ExecutionStage> finalStages,
                                       ExecutionStage successorExecutionStage,
                                       ExecutionTask currentExecutionTask,
                                       HashSet<Object> visitedTasks) {

        // Get or create the final ExecutionStage.
        final InterimStage interimStage = this.assignedInterimStages.get(currentExecutionTask);
        final ExecutionStage executionStage = finalStages.computeIfAbsent(interimStage, InterimStage::toExecutionStage);

        if (successorExecutionStage != null
                && !executionStage.equals(successorExecutionStage)
                && !executionStage.getSuccessors().contains(successorExecutionStage)) {
            executionStage.addSuccessor(successorExecutionStage);
        }

        // Avoid running into loops. However, we must not do this check earlier because we might visit ExecutionTasks
        // from several different predecessor InterimStages.
        if (!visitedTasks.add(currentExecutionTask)) {
            return;
        }

        for (Channel channel : currentExecutionTask.getInputChannels()) {
            if (this.shouldVisitProducerOf(channel)) {
                final ExecutionTask predecessor = channel.getProducer();
                this.assembleExecutionPlan(finalStages, executionStage, predecessor, visitedTasks);
            }
        }

    }

    /**
     * Tells whether the producer of the given {@link Channel} (and its producers in turn) should be considered.
     */
    private boolean shouldVisitProducerOf(Channel channel) {
        // We do not follow copied Channels, because they mark the border to already executed ExecutionTasks.
        return !channel.isCopy();
    }

    /**
     * Intermediate step towards {@link ExecutionStage}s. Supports some build utilites and can be split.
     */
    private interface InterimStage {

        Set<ExecutionTask> getTasks();

        Platform getPlatform();

        void addTask(ExecutionTask task);

        void setOutbound(ExecutionTask task);

        ExecutionStage toExecutionStage();

        InterimStage separate(Set<ExecutionTask> separableTasks);

        /**
         * Check whether this instance is marked to have new dependencies. If so, reset the mark.
         *
         * @return whether this instance was marked
         * @see #markDependenciesUpdated()
         */
        boolean getAndResetSplitMark();

        /**
         * Mark this instance. We use it, to mark instances that have gotten new {@link #requiredStages} during
         * splitting of stages.
         */
        void markDependenciesUpdated();

        Set<ExecutionTask> getOutboundTasks();

        Collection<ExecutionTask> getStartTasks();
    }

    private class InterimStageImpl implements InterimStage {

        /**
         * The {@link PlatformExecution} to that this instance belongs to.
         */
        private final PlatformExecution platformExecution;

        /**
         * All tasks being in this instance.
         */
        private final Set<ExecutionTask> allTasks = new HashSet<>();

        /**
         * All tasks that feed a {@link Channel} that is consumed by a different {@link PlatformExecution}.
         */
        private final Set<ExecutionTask> outboundTasks = new HashSet<>();

        /**
         * Use for mark-and-sweep algorithms. (Specifically: mark changed stages)
         */
        private boolean isMarked;

        /**
         * For debugging purposes only.
         */
        private final int sequenceNumber;

        /**
         * Creates a new instance.
         */
        public InterimStageImpl(PlatformExecution platformExecution) {
            this(platformExecution, 0);
        }

        private InterimStageImpl(PlatformExecution platformExecution, int sequenceNumber) {
            this.platformExecution = platformExecution;
            this.sequenceNumber = sequenceNumber;
        }

        @Override
        public Platform getPlatform() {
            return this.platformExecution.getPlatform();
        }

        @Override
        public void addTask(ExecutionTask task) {
            this.allTasks.add(task);
        }

        @Override
        public void setOutbound(ExecutionTask task) {
            Validate.isTrue(this.allTasks.contains(task));
            this.outboundTasks.add(task);
        }

        @Override
        public Set<ExecutionTask> getOutboundTasks() {
            return this.outboundTasks;
        }

        @Override
        public Collection<ExecutionTask> getStartTasks() {
            return this.getTasks().stream().filter(this::checkIfStartTask).collect(Collectors.toList());
        }

        @Override
        public Set<ExecutionTask> getTasks() {
            return this.allTasks;
        }

        @Override
        public InterimStage separate(Set<ExecutionTask> separableTasks) {
            InterimStage newStage = this.createSplit();
            for (Iterator<ExecutionTask> i = this.allTasks.iterator(); i.hasNext(); ) {
                final ExecutionTask task = i.next();
                if (separableTasks.contains(task)) {
                    i.remove();
                    newStage.addTask(task);
                    if (this.outboundTasks.remove(task)) {
                        newStage.setOutbound(task);
                    }
                }
            }
            // Exchange Channels where necessary.
            for (ExecutionTask task : this.allTasks) {
                for (int outputIndex = 0; outputIndex < task.getNumOuputChannels(); outputIndex++) {
                    Channel outputChannel = task.getOutputChannels()[outputIndex];
                    boolean isInterStageRequired = outputChannel.getConsumers().stream()
                            .anyMatch(consumer -> !this.allTasks.contains(consumer));
                    if (!isInterStageRequired) continue;
                    this.outboundTasks.add(task);
//                    if (outputChannel.isInterStageCapable()) continue;
                    // TODO: We cannot "exchange" Channels so easily any more.
//                    if (!task.getOperator().getPlatform().getChannelManager()
//                            .exchangeWithInterstageCapable(outputChannel)) {
//                        StageAssignmentTraversal.logger.warn("Could not exchange {} with an interstage-capable channel.",
//                                outputChannel);
//                    }
                }
            }

            return newStage;
        }

        public InterimStage createSplit() {
            return new InterimStageImpl(this.platformExecution, this.sequenceNumber + 1);
        }

        @Override
        public void markDependenciesUpdated() {
            this.isMarked = true;
        }

        @Override
        public boolean getAndResetSplitMark() {
            final boolean value = this.isMarked;
            this.isMarked = false;
            return value;
        }

        @Override
        public String toString() {
            return String.format("InterimStage%s", this.getStartTasks());
//            return String.format("InterimStage[%s:%d]", this.getPlatform().getName(), this.sequenceNumber);
        }

        @Override
        public ExecutionStage toExecutionStage() {
            final Iterator<ExecutionTask> iterator = this.allTasks.iterator();
            final LoopSubplan loop = iterator.next().getOperator().getInnermostLoop();
            assert Iterators.allMatch(iterator,
                    task -> task.getOperator().getInnermostLoop() == loop,
                    true
            ) : String.format("There are different loops in the stage with the tasks %s.",
                    this.allTasks.stream()
                            .map(task -> new Tuple<>(task, task.getOperator().getInnermostLoop()))
                            .collect(Collectors.toList())
            );

            ExecutionStageLoop executionStageLoop = null;
            if (loop != null) {
                executionStageLoop = StageAssignmentTraversal.this.stageLoops.computeIfAbsent(loop, ExecutionStageLoop::new);
            }

            final ExecutionStage executionStage = this.platformExecution.createStage(executionStageLoop, this.sequenceNumber);
            for (ExecutionTask task : this.allTasks) {
                executionStage.addTask(task);
                if (this.checkIfStartTask(task)) {
                    executionStage.markAsStartTask(task);
                }
                if (this.checkIfTerminalTask(task)) {
                    executionStage.markAsTerminalTask(task);
                }
            }
            assert !executionStage.getTerminalTasks().isEmpty() :
                    String.format("No terminal tasks among %s.", this.allTasks);
            return executionStage;
        }

        /**
         * Checks if <i>all</i> input {@link Channel}s of the given {@code task} are outbound w.r.t. to the
         * {@link InterimStage}.
         */
        private boolean checkIfStartTask(ExecutionTask task) {
            for (Channel channel : task.getInputChannels()) {
                if (this.checkIfFeedbackChannel(task, channel)) continue;
                final ExecutionTask producer = channel.getProducer();
                if (this.equals(StageAssignmentTraversal.this.assignedInterimStages.get(producer))) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Checks if the given {@code channel} is a feedback to {@code task} (i.e., it closes a data flow cycle).
         */
        private boolean checkIfFeedbackChannel(ExecutionTask task, Channel channel) {
            if (!task.getOperator().isLoopHead()) return false;
            final InputSlot<?> input = task.getInputSlotFor(channel);
            return input != null && input.isFeedback();
        }

        /**
         * Checks if <i>all</i> output {@link Channel}s of the given {@code task} are outbound w.r.t. to the
         * {@link InterimStage}.
         */
        private boolean checkIfTerminalTask(ExecutionTask task) {
            for (Channel channel : task.getOutputChannels()) {
                if (this.checkIfFeedforwardChannel(task, channel)) continue;
                for (ExecutionTask consumer : channel.getConsumers()) {
                    if (this.equals(StageAssignmentTraversal.this.assignedInterimStages.get(consumer))) {
                        return false;
                    }
                }
            }
            return true;
        }

        /**
         * Checks if the given {@code channel} is a feedforward to {@code task} (i.e., it constitutes the beginning of a data flow cycle).
         */
        private boolean checkIfFeedforwardChannel(ExecutionTask task, Channel channel) {
            if (!task.getOperator().isLoopHead()) return false;
            final OutputSlot<?> output = task.getOutputSlotFor(channel);
            return output != null && output.isFeedforward();
        }
    }

//    private static class ExecutionStageAdapter implements InterimStage {
//
//        private final ExecutionStage executionStage;
//
//        private boolean isMarked = false;
//
//        public ExecutionStageAdapter(ExecutionStage executionStage) {
//            this.executionStage = executionStage;
//        }
//
//        @Override
//        public Set<ExecutionTask> getTasks() {
//            return this.executionStage.getAllTasks();
//        }
//
//        @Override
//        public Platform getPlatform() {
//            return this.executionStage.getPlatformExecution().getPlatform();
//        }
//
//        @Override
//        public void addTask(ExecutionTask task) {
//            throw new RuntimeException("Unmodifiable.");
//        }
//
//        @Override
//        public void setOutbound(ExecutionTask task) {
//            throw new RuntimeException("Unmodifiable.");
//        }
//
//        @Override
//        public ExecutionStage toExecutionStage() {
//            return this.executionStage;
//        }
//
//        @Override
//        public InterimStage separate(Set<ExecutionTask> separableTasks) {
//            throw new RuntimeException("Unmodifiable.");
//        }
//
//        @Override
//        public boolean getAndResetSplitMark() {
//            boolean wasMarked = this.isMarked;
//            this.isMarked = false;
//            return wasMarked;
//        }
//
//        @Override
//        public void mark() {
//            this.isMarked = true;
//        }
//
//        @Override
//        public Set<ExecutionTask> getOutboundTasks() {
//            return new HashSet<>(this.executionStage.getTerminalTasks());
//        }
//    }
//

    /**
     * Criterion to futher split {@link InterimStage} besides precedence.
     */
    @FunctionalInterface
    public interface StageSplittingCriterion {

        /**
         * Tells whether two adjacent {@link ExecutionTask}s of the same current {@link InterimStage} should be placed
         * in different {@link ExecutionStage}s.
         *
         * @return {@code true} if the {@link ExecutionTask}s must be separated, {@code false} if it is not necessary
         */
        boolean shouldSplit(ExecutionTask producerTask, Channel channel, ExecutionTask consumerTask);

    }

}
