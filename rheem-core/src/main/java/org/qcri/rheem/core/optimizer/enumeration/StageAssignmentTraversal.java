package org.qcri.rheem.core.optimizer.enumeration;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.plan.executionplan.*;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.LoopSubplan;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.Iterators;
import org.qcri.rheem.core.util.OneTimeExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Builds an {@link ExecutionPlan} from a {@link ExecutionTaskFlow}.
 * <p>Specifically, subdivides the {@link ExecutionTask}s into {@link PlatformExecution} and {@link ExecutionStage}s,
 * thereby discarding already executed {@link ExecutionTask}s. As of now, these are recognized as producers of
 * {@link Channel}s that are copied. This is because of {@link ExecutionTaskFlowCompiler} that copies {@link Channel}s
 * to different alternative {@link ExecutionPlan}s on top of existing fixed {@link ExecutionTask}s.</p>
 */
public class StageAssignmentTraversal extends OneTimeExecutable {

    private static final Logger logger = LoggerFactory.getLogger(StageAssignmentTraversal.class);

    /**
     * Should be turned into a {@link ExecutionPlan}.
     */
    private final ExecutionTaskFlow executionTaskFlow;

    /**
     * Maintains {@link ExecutionTask}s that might not yet be assigned to an {@link InterimStage} and must be explored.
     */
    private Queue<ExecutionTask> seeds;

    /**
     * Assigns {@link ExecutionTask}s with {@link InterimStage}s.
     */
    private Map<ExecutionTask, InterimStage> assignedInterimStages;

    /**
     * Keeps track of {@link InterimStage}s that must be executed before executing a certain {@link ExecutionTask}.
     */
    private Map<ExecutionTask, Set<InterimStage>> requiredStages;

    /**
     * Zero or more {@link StageSplittingCriterion}s to further refine {@link ExecutionStage}s.
     */
    private Collection<StageSplittingCriterion> additionalSplittingCriteria;

    /**
     * All {@link InterimStage}s created by this instance.
     */
    private Collection<InterimStage> allStages;

    /**
     * Newly created {@link InterimStage}s that might be subject to refinement still.
     */
    private Collection<InterimStage> newStages;

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
     * @param executionTaskFlow           should be converted into a {@link ExecutionPlan}
     * @param additionalSplittingCriteria to create splits beside the precedence-based splitting
     */
    StageAssignmentTraversal(ExecutionTaskFlow executionTaskFlow,
                             StageSplittingCriterion... additionalSplittingCriteria) {
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
        this.executionTaskFlow = executionTaskFlow;

        this.additionalSplittingCriteria = new ArrayList<>();
        this.additionalSplittingCriteria.add(StageAssignmentTraversal::isLoopBoarder);
        this.additionalSplittingCriteria.addAll(Arrays.asList(additionalSplittingCriteria));
    }

    public static ExecutionPlan assignStages(ExecutionTaskFlow executionTaskFlow,
                                             StageSplittingCriterion... additionalSplittingCriteria) {
        final StageAssignmentTraversal instance = new StageAssignmentTraversal(executionTaskFlow, additionalSplittingCriteria);
        return instance.buildExecutionPlan();
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
        this.initializeRun();
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
     * Sets up fields.
     */
    private void initializeRun() {
        this.seeds = new LinkedList<>();
        this.assignedInterimStages = new HashMap<>();
        this.requiredStages = new HashMap<>();
        this.newStages = new LinkedList<>();
        this.allStages = new LinkedList<>();
    }

    /**
     * Create an initial assignment for each {@link ExecutionTask}, thereby keeping track of existing
     * {@link PlatformExecution}s.
     */
    private void discoverInitialStages() {
        // ExecutionTasks which have to be assigned an InterimStage.
        final Set<ExecutionTask> relevantTasks = new HashSet<>();

        // ExecutionTasks that staged for exploration.
        final Queue<ExecutionTask> stagedTasks = new LinkedList<>(this.executionTaskFlow.getSinkTasks());

        // Run until all ExecutionTasks have been checked.
        while (!stagedTasks.isEmpty()) {
            final ExecutionTask task = stagedTasks.poll();
            if (!relevantTasks.add(task)) continue;

            for (Channel inputChannel : task.getInputChannels()) {
                if (!this.shouldVisitProducerOf(inputChannel)) { // Barrier.
                    final ExecutionTask producer = inputChannel.getProducer();
                    // Most important seeds are those that might need to use an existing PlatformExecution:
                    /// We need to create their InterimStages immediately to reuse the PlatformExecution before
                    /// ExecutionTasks are assigned to other InterimStages.
                    if (producer.getPlatform().equals(task.getPlatform()) &&
                            producer.getPlatform().isSinglePlatformExecutionPossible(producer, inputChannel, task)) {
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
        this.seeds.addAll(relevantTasks);
        while (!this.seeds.isEmpty()) {
            final ExecutionTask task = this.seeds.poll();
            this.createStageFor(task, null);
        }

        // Make sure that we didn't blunder.
        assert relevantTasks.stream().allMatch(this.assignedInterimStages::containsKey);
    }

    /**
     * Starts building an {@link InterimStage} starting from the given {@link ExecutionTask}. If a {@link PlatformExecution}
     * is provided, the {@link InterimStage} will be associated with it. Eventually, also {@link #seeds} will be planted
     * for adjacent {@link InterimStage}s.
     */
    private void createStageFor(ExecutionTask task, PlatformExecution platformExecution) {
        if (this.assignedInterimStages.containsKey(task)) {
            return;
        }
        Platform platform = task.getOperator().getPlatform();
        if (task.getStage() == null) {
            if (platformExecution == null) {
                platformExecution = new PlatformExecution(platform);
            }
            InterimStage initialStage = new InterimStageImpl(platformExecution);
            this.addStage(initialStage);
            this.traverseTask(task, initialStage);
        }
    }

    /**
     * Adds an {@link InterimStage} byt putting it on {@link #allStages} and {@link #newStages}.
     */
    private void addStage(InterimStage stage) {
        this.newStages.add(stage);
        this.allStages.add(stage);
    }

    /**
     * Assign the given {@link ExecutionTask} to the given {@link InterimStage}s, then handle adjacent {@link ExecutionTask}s.
     */
    private void traverseTask(ExecutionTask task, InterimStage interimStage) {
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
        logger.debug("Reassigned %s from %s to %s.", task, oldStage, newStage);
        final Set<InterimStage> thisRequiredStages = this.requiredStages.computeIfAbsent(
                task, key -> new HashSet<>(4)
        );
        thisRequiredStages.add(newStage);
        logger.debug("Assigning {} to {}.", task, newStage);
    }

    /**
     * Handle the downstream neighbors of the given {@code task}. Either expand the {@code expandableStage} or
     * plant new {@link #seeds}.
     */
    private void expandDownstream(ExecutionTask task, InterimStage expandableStage) {
        for (Channel channel : task.getOutputChannels()) {
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
     * Handle the upstream neighbors of the given {@code task}. Either expand the {@code expandableStage} or
     * plant new {@link #seeds}.
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
     * Handle a given {@code task}. Either expand the {@code expandableStage} or plant new {@link #seeds}.
     */
    private void handleTaskWithoutPlatformExecution(ExecutionTask task, InterimStage expandableStage) {
        final Platform operatorPlatform = task.getOperator().getPlatform();
        if (expandableStage != null && operatorPlatform.equals(expandableStage.getPlatform())) {
            this.traverseTask(task, expandableStage);
        } else {
            this.seeds.add(task);
        }
    }

    /**
     * Refine the current {@link #newStages} and put the result to {@link #allStages}.
     */
    private void refineStages() {
        // Apply the #additionalSplittingCriteria at first.
        for (InterimStage stage : new ArrayList<>(this.newStages)) {
            this.applySplittingCriteria(stage);
        }

        while (!this.newStages.isEmpty()) {
            // Update the precedence graph.
            for (InterimStage currentStage : this.newStages) {
                for (ExecutionTask outboundTask : currentStage.getOutboundTasks()) {
                    final HashSet<InterimStage> requiredStages = new HashSet<>();
                    requiredStages.addAll(this.requiredStages.get(outboundTask));
                    for (Channel channel : outboundTask.getOutputChannels()) {
                        for (ExecutionTask consumer : channel.getConsumers()) {
                            this.updateRequiredStages(consumer, requiredStages);
                        }
                    }
                }
            }

            // Partition stages. Might yield #newStages.
            this.newStages.clear();
            for (InterimStage stage : new ArrayList<>(this.allStages)) {
                this.partitionStage(stage);
            }
        }
    }

    /**
     * Applies all {@link #additionalSplittingCriteria} to the given {@code stage}, thereby refining it.
     */
    private void applySplittingCriteria(InterimStage stage) {
        Set<ExecutionTask> separableTasks = new HashSet<>();
        Queue<ExecutionTask> taskQueue = new LinkedList<>(stage.getStartTasks());
        Set<ExecutionTask> seenTasks = new HashSet<>();
        while (!taskQueue.isEmpty()) {
            final ExecutionTask task = taskQueue.poll();
            boolean isSplittableTask = separableTasks.contains(task);
            if (seenTasks.add(task)) {
                for (Channel channel : task.getOutputChannels()) {
                    for (ExecutionTask consumerTask : channel.getConsumers()) {
                        if (this.assignedInterimStages.get(consumerTask) != stage) {
                            continue;
                        }
                        if (isSplittableTask || this.additionalSplittingCriteria.stream().anyMatch(
                                criterion -> criterion.shouldSplit(task, channel, consumerTask)
                        )) {
                            separableTasks.add(consumerTask);
                        }
                        taskQueue.add(consumerTask);
                    }
                }
            }
        }

        if (!separableTasks.isEmpty()) {
            if (separableTasks.size() < stage.getTasks().size()) {
                final InterimStage separatedStage = this.splitStage(stage, separableTasks);
                this.applySplittingCriteria(separatedStage);
            } else {
                // TODO: How can this happen?
                logger.warn(
                        "Cannot separate {} tasks from stage with {} tasks! Proceed without splitting...",
                        separableTasks.size(), stage.getTasks().size());
            }
        }
    }

    /**
     * Update the {@link #requiredStages} while recursively traversing downstream over {@link ExecutionTask}s.
     * Also, mark its {@link InterimStage} ({@link InterimStage#mark()})
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
        if (!requiredStages.contains(currentStage)) {
            requiredStages = new HashSet<>(requiredStages);
            requiredStages.add(currentStage);
        }

        // Update the requiredStages of the task. On change, propagate the requirements downstream.
        final Set<InterimStage> currentlyRequiredStages = this.requiredStages.get(task);
        if (currentlyRequiredStages.addAll(requiredStages)) {
            logger.debug("Updated required stages of {} to {}.", task, currentlyRequiredStages);
            currentStage.mark();

            for (Channel channel : task.getOutputChannels()) {
                for (ExecutionTask consumingTask : channel.getConsumers()) {
                    this.updateRequiredStages(consumingTask, requiredStages);
                }
            }
        }
    }


    /**
     * Partition the {@code stage} into two halves. All {@link ExecutionTask}s that do not have the minimum count of
     * required {@link InterimStage}s will be put into a new {@link InterimStage}.
     */
    private boolean partitionStage(InterimStage stage) {
        // Short-cut: if the stage has not been marked, its required stages did not change.
        if (!stage.getAndResetMark()) {
            return false;
        }
        int minRequiredStages = -1;
        final Collection<ExecutionTask> initialTasks = new LinkedList<>();
        final Set<ExecutionTask> separableTasks = new HashSet<>();
        for (ExecutionTask task : stage.getTasks()) {
            final Set<InterimStage> requiredStages = this.requiredStages.get(task);
            if (minRequiredStages == -1 || requiredStages.size() < minRequiredStages) {
                separableTasks.addAll(initialTasks);
                initialTasks.clear();
                minRequiredStages = requiredStages.size();
            }
            (minRequiredStages == requiredStages.size() ? initialTasks : separableTasks).add(task);
        }

        if (separableTasks.isEmpty()) {
            logger.debug("No separable tasks found in marked stage {}.", stage);
            return false;
        } else {
            this.splitStage(stage, separableTasks);
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
        logger.debug("Separating " + separableTasks + " from " + stage + "...");
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

        boolean getAndResetMark();

        void mark();

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
                    if (outputChannel.isInterStageCapable()) continue;
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
        public void mark() {
            this.isMarked = true;
        }

        @Override
        public boolean getAndResetMark() {
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
//        public boolean getAndResetMark() {
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
