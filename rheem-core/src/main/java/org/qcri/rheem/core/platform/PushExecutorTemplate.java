package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.executionplan.PlatformExecution;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.LoopHeadOperator;
import org.qcri.rheem.core.util.OneTimeExecutable;
import org.qcri.rheem.core.util.RheemCollections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * {@link Executor} implementation that employs a push model, i.e., data quanta are "pushed"
 * through the {@link ExecutionStage}.
 */
public abstract class PushExecutorTemplate<CI extends ChannelInstance> implements Executor {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    private Collection<ExecutionResource> allocatedResources = new LinkedList<>();

    private Map<Channel, CI> interStageChannelInstances = new HashMap<>(4);

    @Override
    public ExecutionProfile execute(ExecutionStage stage) {
        final StageExecution stageExecution = new StageExecution(stage);
        return stageExecution.executeStage();
    }


    /**
     * Executes an {@link ExecutionTask}.
     *
     * @param taskActivator    provides the {@link ExecutionTask} and its input dependencies.
     * @param isForceExecution whether execution is forced, i.e., lazy execution of {@link ExecutionTask} is prohibited
     * @return the output {@link ChannelInstance}s of the {@link ExecutionTask}
     */
    private Collection<CI> execute(TaskActivator taskActivator, boolean isForceExecution) {
        // Execute the ExecutionTask.
        this.open(taskActivator.getTask(), taskActivator.getInputChannelInstances());
        final List<CI> outputChannelInstances = this.execute(
                taskActivator.getTask(),
                taskActivator.getInputChannelInstances(),
                isForceExecution
        );

        for (CI outputChannelInstance : outputChannelInstances) {
            if (outputChannelInstance == null) continue;

            // Register the outputChannelInstances as allocated resources, so that we can clean up later.
            this.allocatedResources.add(outputChannelInstance);

            // Keep inter-stage ChannelInstances around.
            this.keepIfInterStageChannelInstance(outputChannelInstance);
        }

        return outputChannelInstances;
    }

    /**
     * Prepares the given {@code task} for execution.
     *
     * @param task                  that should be executed
     * @param inputChannelInstances inputs into the {@code task}
     * @return the {@link ChannelInstance}s created as output of {@code task}
     */
    protected abstract void open(ExecutionTask task, List<CI> inputChannelInstances);

    /**
     * Executes the given {@code task} and return the output {@link ChannelInstance}s.
     *
     * @param task                  that should be executed
     * @param inputChannelInstances inputs into the {@code task}
     * @param isForceExecution      forbids lazy execution
     * @return the {@link ChannelInstance}s created as output of {@code task}
     */
    protected abstract List<CI> execute(ExecutionTask task, List<CI> inputChannelInstances, boolean isForceExecution);

    /**
     * If the given {@code channelInstance} is needed later on, keep it around in {@link #interStageChannelInstances}.
     * @param channelInstance
     */
    private void keepIfInterStageChannelInstance(CI channelInstance) {
        // Reckon with null values (cf. LoopHeadOperator).
        if (channelInstance == null) return;

        // Find out which ExecutionStage is currently dealt with.
        final Channel channel = channelInstance.getChannel();
        final ExecutionStage currentStage = channel.getProducer().getStage();
        final PlatformExecution currentPlatformExecution = currentStage.getPlatformExecution();

        // Keep the ChannelInstance if there is any other ExecutionStage from the same PlatformExecution requiring it.
        if (channel.getConsumers().stream().anyMatch(
                consumer ->
                        consumer.getStage() != currentStage
                                && consumer.getStage().getPlatformExecution() == currentPlatformExecution
        )) {
            this.interStageChannelInstances.put(channel, channelInstance);
        }

    }


    @Override
    public void dispose() {
        this.allocatedResources.forEach(ExecutionResource::release);
        this.allocatedResources.clear();
    }

    /**
     * Keeps track of state that is required within the execution of a single {@link ExecutionStage}. Specifically,
     * it issues to the {@link PushExecutorTemplate}, which {@link ExecutionTask}s should be executed in which
     * order with which input dependencies.
     */
    protected class StageExecution extends OneTimeExecutable {

        private final Map<ExecutionTask, TaskActivator> stagedActivators = new HashMap<>();

        private final Queue<TaskActivator> readyActivators = new LinkedList<>();

        private final Set<ExecutionTask> terminalTasks;

        private final Collection<ChannelInstance> allChannelInstances = new LinkedList<>();

        private StageExecution(ExecutionStage stage) {
            // Initialize the readyActivators.
            assert !stage.getStartTasks().isEmpty() : String.format("No start tasks for {}.", stage);
            for (ExecutionTask startTask : stage.getStartTasks()) {
                TaskActivator activator = new TaskActivator(startTask);
                assert activator.isReady() : String.format("Stage starter %s is not immediately ready.", startTask);
                this.readyActivators.add(activator);
            }

            // Initialize the terminalTasks.
            this.terminalTasks = RheemCollections.asSet(stage.getTerminalTasks());
        }

        public ExecutionProfile executeStage() {
            this.execute();
            return this.assembleExecutionProfile();
        }

        @Override
        protected void doExecute() {
            TaskActivator readyActivator;
            while ((readyActivator = this.readyActivators.poll()) != null) {
                // Execute the ExecutionTask.
                final ExecutionTask task = readyActivator.getTask();
                final Collection<CI> outputChannelInstances = this.execute(readyActivator, task);

                // Register the outputChannelInstances (to obtain cardinality measurements).
                outputChannelInstances.stream().filter(Objects::nonNull).forEach(this.allChannelInstances::add);

                // Activate successor ExecutionTasks.
                this.activateSuccessorTasks(task, outputChannelInstances);
            }
        }

        private Collection<CI> execute(TaskActivator readyActivator, ExecutionTask task) {
            final boolean isForceExecution = this.terminalTasks.contains(task);
            return this.executor().execute(readyActivator, isForceExecution);
        }

        private void activateSuccessorTasks(ExecutionTask task, Collection<CI> outputChannelInstances) {
            for (CI outputChannelInstance : outputChannelInstances) {
                if (outputChannelInstance == null) continue; // Partial results possible (cf. LoopHeadOperator).

                final Channel channel = outputChannelInstance.getChannel();
                for (ExecutionTask consumer : channel.getConsumers()) {
                    if (consumer.getStage() != task.getStage()) continue; // Stay within ExecutionStage.

                    // Get or create the TaskActivator.
                    final TaskActivator consumerActivator = this.stagedActivators.computeIfAbsent(consumer, TaskActivator::new);

                    // Register the outputChannelInstance.
                    consumerActivator.accept(outputChannelInstance);

                    // Schedule the consumerActivator if it isReady.
                    if (consumerActivator.isReady()) {
                        this.stagedActivators.remove(consumer);
                        this.readyActivators.add(consumerActivator);
                    }
                }
            }
        }

        private PushExecutorTemplate<CI> executor() {
            return PushExecutorTemplate.this;
        }

        private ExecutionProfile assembleExecutionProfile() {
            ExecutionProfile executionProfile = new ExecutionProfile();
            final Map<Channel, Long> cardinalities = executionProfile.getCardinalities();
            for (final ChannelInstance channelInstance : this.allChannelInstances) {
                final OptionalLong measuredCardinality = channelInstance.getMeasuredCardinality();
                if (measuredCardinality.isPresent()) {
                    cardinalities.put(channelInstance.getChannel(), measuredCardinality.getAsLong());
                } else if (channelInstance.getChannel().isMarkedForInstrumentation()) {
                    PushExecutorTemplate.this.logger.warn(
                            "No cardinality available for {}, although it was requested.", channelInstance.getChannel()
                    );
                }
            }
            return executionProfile;
        }
    }

    protected class TaskActivator {

        private final ExecutionTask task;

        private final ArrayList<CI> inputChannelInstances;

        protected TaskActivator(ExecutionTask task) {
            this.task = task;
            this.inputChannelInstances = RheemCollections.createNullFilledArrayList(this.getOperator().getNumInputs());
            this.acceptInterStageChannelInstances();
        }

        private void acceptInterStageChannelInstances() {
            for (int inputIndex = 0; inputIndex < this.task.getNumInputChannels(); inputIndex++) {
                final Channel channel = this.task.getInputChannel(inputIndex);
                final CI channelInstance = PushExecutorTemplate.this.interStageChannelInstances.get(channel);
                if (channelInstance != null) {
                    this.accept(channelInstance);
                }
            }
        }

        /**
         * Registers the {@code inputChannelInstance} as input of the wrapped {@link #task}.
         *
         * @param inputChannelInstance the input {@link ChannelInstance}
         */
        public void accept(CI inputChannelInstance) {
            // Identify the input index of the inputChannelInstance wrt. the ExecutionTask.
            final Channel channel = inputChannelInstance.getChannel();
            final InputSlot<?> inputSlot = this.task.getInputSlotFor(channel);
            assert inputSlot != null
                    : String.format("Could not identify an InputSlot in %s for %s.", this.task, inputChannelInstance);
            final int inputIndex = inputSlot.getIndex();

            // Register the inputChannelInstance.
            assert this.inputChannelInstances.get(inputIndex) == null
                    : String.format("Tried to replace %s with %s as %dth input of %s.",
                    this.inputChannelInstances.get(inputIndex), inputChannelInstance, inputIndex, this.task);
            this.inputChannelInstances.set(inputIndex, inputChannelInstance);

        }

        public ExecutionTask getTask() {
            return this.task;
        }

        protected ExecutionOperator getOperator() {
            return this.task.getOperator();
        }

        protected List<CI> getInputChannelInstances() {
            return this.inputChannelInstances;
        }

        /**
         * @return whether the {@link #task} has sufficient input dependencies satisfied
         */
        protected boolean isReady() {
            return this.isLoopInitializationReady() || this.isLoopIterationReady() || this.isPlainReady();
        }

        /**
         * @return whether the {@link #task} has all input dependencies satisfied
         */
        protected boolean isPlainReady() {
            for (InputSlot<?> inputSlot : this.getOperator().getAllInputs()) {
                if (this.inputChannelInstances.get(inputSlot.getIndex()) == null) return false;
            }
            return true;
        }

        /**
         * @return whether the {@link #task} has all loop initialization input dependencies satisfied
         */
        protected boolean isLoopInitializationReady() {
            if (!this.getOperator().isLoopHead()) return false;
            final LoopHeadOperator loopHead = (LoopHeadOperator) this.getOperator();
            if (loopHead.getState() != LoopHeadOperator.State.NOT_STARTED) return false;
            for (InputSlot<?> inputSlot : loopHead.getLoopInitializationInputs()) {
                if (this.inputChannelInstances.get(inputSlot.getIndex()) == null) return false;
            }
            return true;
        }

        /**
         * @return whether the {@link #task} has all loop iteration input dependencies satisfied
         */
        protected boolean isLoopIterationReady() {
            if (!this.getOperator().isLoopHead()) return false;
            final LoopHeadOperator loopHead = (LoopHeadOperator) this.getOperator();
            if (loopHead.getState() != LoopHeadOperator.State.RUNNING) return false;
            for (InputSlot<?> inputSlot : loopHead.getLoopBodyInputs()) {
                if (this.inputChannelInstances.get(inputSlot.getIndex()) == null) return false;
            }
            return true;
        }
    }
}
