package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.LoopHeadOperator;
import org.qcri.rheem.core.util.Formats;
import org.qcri.rheem.core.util.OneTimeExecutable;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * {@link Executor} implementation that employs a push model, i.e., data quanta are "pushed"
 * through the {@link ExecutionStage}.
 */
public abstract class PushExecutorTemplate extends ExecutorTemplate {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected final Job job;

    public PushExecutorTemplate(Job job) {
        super(job == null ? null : job.getCrossPlatformExecutor());
        this.job = job;
    }

    @Override
    public void execute(ExecutionStage stage, OptimizationContext optimizationContext, ExecutionState executionState) {
        assert !this.isDisposed() : String.format("%s has been disposed.", this);

        final StageExecution stageExecution = new StageExecution(stage, optimizationContext, executionState);
        stageExecution.executeStage();
    }


    /**
     * Executes an {@link ExecutionTask}.
     *
     * @param taskActivator    provides the {@link ExecutionTask} and its input dependenchannelInstancees.
     * @param isForceExecution whether execution is forced, i.e., lazy execution of {@link ExecutionTask} is prohibited
     * @return the output {@link ChannelInstance}s of the {@link ExecutionTask}
     */
    private Tuple<List<ChannelInstance>, PartialExecution> execute(TaskActivator taskActivator, boolean isForceExecution) {
        // Execute the ExecutionTask.
        this.open(taskActivator.getTask(), taskActivator.getInputChannelInstances());

        return this.execute(
                taskActivator.getTask(),
                taskActivator.getInputChannelInstances(),
                taskActivator.getOperatorContext(),
                isForceExecution
        );
    }

    /**
     * Utility method to create the output {@link ChannelInstance}s for a certain {@link ExecutionTask}.
     *
     * @param task                    the {@link ExecutionTask}
     * @param producerOperatorContext the {@link OptimizationContext.OperatorContext} for the {@link ExecutionTask}
     * @param inputChannelInstances   the input {@link ChannelInstance}s for the {@code task}
     * @return
     */
    protected ChannelInstance[] createOutputChannelInstances(ExecutionTask task,
                                                             OptimizationContext.OperatorContext producerOperatorContext,
                                                             List<ChannelInstance> inputChannelInstances) {
        ChannelInstance[] channelInstances = new ChannelInstance[task.getNumOuputChannels()];
        for (int outputIndex = 0; outputIndex < channelInstances.length; outputIndex++) {
            final Channel outputChannel = task.getOutputChannel(outputIndex);
            final ChannelInstance outputChannelInstance = outputChannel.createInstance(this, producerOperatorContext, outputIndex);
            channelInstances[outputIndex] = outputChannelInstance;
            for (ChannelInstance inputChannelInstance : inputChannelInstances) {
                if (inputChannelInstance != null) {
                    outputChannelInstance.addPredecessor(inputChannelInstance);
                }
            }
        }
        return channelInstances;
    }

    /**
     * Prepares the given {@code task} for execution.
     *
     * @param task                  that should be executed
     * @param inputChannelInstances inputs into the {@code task}
     * @return the {@link ChannelInstance}s created as output of {@code task}
     */
    protected abstract void open(ExecutionTask task, List<ChannelInstance> inputChannelInstances);

    /**
     * Executes the given {@code task} and return the output {@link ChannelInstance}s.
     *
     * @param task                    that should be executed
     * @param inputChannelInstances   inputs into the {@code task}
     * @param producerOperatorContext
     * @param isForceExecution        forbids lazy execution  @return the {@link ChannelInstance}s created as output of {@code task}
     */
    protected abstract Tuple<List<ChannelInstance>, PartialExecution> execute(ExecutionTask task,
                                                                              List<ChannelInstance> inputChannelInstances,
                                                                              OptimizationContext.OperatorContext producerOperatorContext,
                                                                              boolean isForceExecution);

    /**
     * If the {@link ExecutionTask} is not executed lazily, then gather all pending executions in a
     * {@link PartialExecution}.
     *
     * @param task                    that was just executed
     * @param inputChannelInstances   that feed the {@code task}
     * @param producerOperatorContext that contains estimates for the {@code task}'s {@link ExecutionOperator}
     * @param outputChannelInstances  that were produces by the {@code task}
     * @param executionDuration       that was measured for the {@link PartialExecution}
     * @return the {@link PartialExecution} or {@code null} if nothing has been executed
     */
    protected PartialExecution handleLazyChannelLineage(ExecutionTask task,
                                                        List<ChannelInstance> inputChannelInstances,
                                                        OptimizationContext.OperatorContext producerOperatorContext,
                                                        ChannelInstance[] outputChannelInstances,
                                                        long executionDuration) {

        if (task.getOperator().isExecutedLazily()) {
            return null;
        }

        List<OptimizationContext.OperatorContext> operatorContexts = new ArrayList<>();
        ChannelInstance[] channelInstances;
        if (outputChannelInstances.length > 0) {
            channelInstances = outputChannelInstances;
        } else {
            channelInstances = inputChannelInstances.toArray(new ChannelInstance[inputChannelInstances.size()]);
            operatorContexts.add(producerOperatorContext);
        }
        for (ChannelInstance channelInstance : channelInstances) {
            operatorContexts.addAll(
                    channelInstance
                            .getLazyChannelLineage()
                            .traverseAndMark(
                                    new LinkedList<>(),
                                    (accu, node) -> RheemCollections.add(accu, node.getProducerOperatorContext())
                            ));
        }
        Collections.reverse(operatorContexts);

        final PartialExecution partialExecution = new PartialExecution(executionDuration, operatorContexts);
        if (this.logger.isInfoEnabled()) {
            this.logger.info(
                    "Executed {} operator(s) in {} (estimated {}): {}",
                    operatorContexts.size(),
                    Formats.formatDuration(partialExecution.getMeasuredExecutionTime()),
                    partialExecution.getOverallTimeEstimate(),
                    partialExecution.getOperatorContexts().stream()
                            .map(opCtx -> String.format("%s->%s", opCtx.getOperator(), opCtx.getTimeEstimate()))
                            .collect(Collectors.toList())
            );
        }

        return partialExecution;
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

        /**
         * State from preceeding executions.
         */
        private final ExecutionState executionState;

        /**
         * Contains additional information of {@link ExecutionOperator}s in the {@link ExecutionStage}.
         */
        private final OptimizationContext optimizationContext;

        private StageExecution(ExecutionStage stage, OptimizationContext optimizationContext, ExecutionState executionState) {
            this.executionState = executionState;
            this.optimizationContext = optimizationContext;

            // Initialize the readyActivators.
            assert !stage.getStartTasks().isEmpty() : String.format("No start tasks for {}.", stage);
            stage.getStartTasks().forEach(this::scheduleStartTask);

            // Initialize the terminalTasks.
            this.terminalTasks = RheemCollections.asSet(stage.getTerminalTasks());
        }

        private void scheduleStartTask(ExecutionTask startTask) {
            TaskActivator activator = new TaskActivator(startTask, this.fetchOperatorContext(startTask), this.executionState);
            assert activator.isReady() : String.format("Stage starter %s is not immediately ready.", startTask);
            this.readyActivators.add(activator);
        }

        /**
         * Fetches the {@link OptimizationContext.OperatorContext} for the given {@link ExecutionTask}.
         *
         * @param task the {@link ExecutionTask}
         * @return the {@link OptimizationContext.OperatorContext}
         */
        private OptimizationContext.OperatorContext fetchOperatorContext(ExecutionTask task) {
            final OptimizationContext.OperatorContext opCtx = this.optimizationContext.getOperatorContext(task.getOperator());
            if (opCtx == null) {
                logger.warn("No OperatorContext for {} available.", task);
            }
            return opCtx;
        }

        /**
         * Executes the {@link ExecutionStage} and contributes results to the {@link #executionState}.
         */
        void executeStage() {
            this.execute();
            this.updateExecutionState();
        }

        @Override
        protected void doExecute() {
            TaskActivator readyActivator;
            while ((readyActivator = this.readyActivators.poll()) != null) {
                // Execute the ExecutionTask.
                final ExecutionTask task = readyActivator.getTask();
                final Tuple<List<ChannelInstance>, PartialExecution> executionResult = this.execute(readyActivator, task);
                readyActivator.dispose();

                // Register the outputChannelInstances (to obtain cardinality measurements and for furhter stages).
                final List<ChannelInstance> outputChannelInstances = executionResult.getField0();
                outputChannelInstances.stream().filter(Objects::nonNull).forEach(this::store);

                // Log executions.
                final PartialExecution partialExecution = executionResult.getField1();
                if (partialExecution != null) {
                    this.executionState.add(partialExecution);
                }

                // Activate successor ExecutionTasks.
                this.activateSuccessorTasks(task, outputChannelInstances);
            }
        }

        /**
         * Puts an {@link ExecutionTask} into execution.
         *
         * @param readyActivator activated the {@code task}
         * @param task           should be executed
         * @return the {@link ChannelInstance}s created by the {@code task} and a {@link PartialExecution} or
         * {@code null} if something has been actually executed
         */
        private Tuple<List<ChannelInstance>, PartialExecution> execute(TaskActivator readyActivator, ExecutionTask task) {
            final boolean isForceExecution = this.terminalTasks.contains(task);
            return this.executor().execute(readyActivator, isForceExecution);
        }

        /**
         * Stores the {@link ChannelInstance}.
         *
         * @param channelInstance should be stored
         */
        private void store(ChannelInstance channelInstance) {
            this.allChannelInstances.add(channelInstance);
            channelInstance.noteObtainedReference();
        }

        private void activateSuccessorTasks(ExecutionTask task, Collection<ChannelInstance> outputChannelInstances) {
            for (ChannelInstance outputChannelInstance : outputChannelInstances) {
                if (outputChannelInstance == null) continue; // Partial results possible (cf. LoopHeadOperator).

                final Channel channel = outputChannelInstance.getChannel();
                for (ExecutionTask consumer : channel.getConsumers()) {
                    // Stay within ExecutionStage.
                    if (consumer.getStage() != task.getStage() || channel.isStageExecutionBarrier()) continue;

                    // Get or create the TaskActivator.
                    final TaskActivator consumerActivator = this.stagedActivators.computeIfAbsent(
                            consumer, (key) -> new TaskActivator(key, this.fetchOperatorContext(key), this.executionState)
                    );

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

        private PushExecutorTemplate executor() {
            return PushExecutorTemplate.this;
        }

        /**
         * Put new {@link ChannelInstance}s to the {@link #executionState} and release input {@link ChannelInstance}s.
         */
        private void updateExecutionState() {
            for (final ChannelInstance channelInstance : this.allChannelInstances) {
                // Capture outbound ChannelInstances.
                if (channelInstance.getChannel().isBetweenStages() || channelInstance.getChannel().isStageExecutionBarrier()) {
                    this.executionState.register(channelInstance);
                }

                // Try to store cardinalities.
                PushExecutorTemplate.this.addCardinalityIfNotInLoop(channelInstance);

                // Release the ChannelInstance.
                channelInstance.noteDiscardedReference(true);
            }
            this.allChannelInstances.clear();
        }
    }

    /**
     * Wraps an {@link ExecutionTask} and collects its input dependencies (i.e., {@link ChannelInstance}s). Then,
     * allows for execution of the {@link ExecutionTask}.
     */
    private class TaskActivator {

        private final ExecutionTask task;

        private final ArrayList<ChannelInstance> inputChannelInstances;

        private final OptimizationContext.OperatorContext operatorContext;

        TaskActivator(ExecutionTask task, OptimizationContext.OperatorContext operatorContext, ExecutionState executionState) {
            assert operatorContext == null || task.getOperator() == operatorContext.getOperator() :
                    String.format("Mismatch between %s and %s.", task, operatorContext);

            this.task = task;
            this.operatorContext = operatorContext;
            this.inputChannelInstances = RheemCollections.createNullFilledArrayList(this.getOperator().getNumInputs());
            this.acceptFrom(executionState);
        }

        /**
         * Accept input {@link ChannelInstance}s from the given {@link ExecutionState}.
         *
         * @param executionState provides {@link ChannelInstance}s
         */
        private void acceptFrom(ExecutionState executionState) {
            for (int inputIndex = 0; inputIndex < this.task.getNumInputChannels(); inputIndex++) {
                final Channel channel = this.task.getInputChannel(inputIndex);
                final ChannelInstance channelInstance = executionState.getChannelInstance(channel);
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
        public void accept(ChannelInstance inputChannelInstance) {
            // Identify the input index of the inputChannelInstance wrt. the ExecutionTask.
            final Channel channel = inputChannelInstance.getChannel();
            final int inputIndex;
            if (this.task.getOperator().getNumInputs() == 0) {
                // If we have a ChannelInstance but no InputSlots, we fall-back to index 0 as per convention.
                assert this.inputChannelInstances.isEmpty();
                this.inputChannelInstances.add(null);
                inputIndex = 0;
            } else {
                final InputSlot<?> inputSlot = this.task.getInputSlotFor(channel);
                assert inputSlot != null
                        : String.format("Could not identify an InputSlot in %s for %s.", this.task, inputChannelInstance);
                inputIndex = inputSlot.getIndex();

            }
            // Register the inputChannelInstance (and check for conflicts).
            assert this.inputChannelInstances.get(inputIndex) == null
                    : String.format("Tried to replace %s with %s as %dth input of %s.",
                    this.inputChannelInstances.get(inputIndex), inputChannelInstance, inputIndex, this.task);
            this.inputChannelInstances.set(inputIndex, inputChannelInstance);
            inputChannelInstance.noteObtainedReference();

        }

        public ExecutionTask getTask() {
            return this.task;
        }

        protected ExecutionOperator getOperator() {
            return this.task.getOperator();
        }

        public OptimizationContext.OperatorContext getOperatorContext() {
            return operatorContext;
        }

        protected List<ChannelInstance> getInputChannelInstances() {
            return this.inputChannelInstances;
        }

        /**
         * Disposes this instance.
         */
        protected void dispose() {
            // Drop references towards the #inputChannelInstances.
            for (ChannelInstance inputChannelInstance : this.inputChannelInstances) {
                if (inputChannelInstance != null) {
                    inputChannelInstance.noteDiscardedReference(true);
                }
            }
        }

        /**
         * @return whether the {@link #task} has suffichannelInstanceent input dependenchannelInstancees satisfied
         */
        protected boolean isReady() {
            return this.isLoopInitializationReady() || this.isLoopIterationReady() || this.isPlainReady();
        }

        /**
         * @return whether the {@link #task} has all input dependenchannelInstancees satisfied
         */
        protected boolean isPlainReady() {
            for (InputSlot<?> inputSlot : this.getOperator().getAllInputs()) {
                if (this.inputChannelInstances.get(inputSlot.getIndex()) == null) return false;
            }
            return true;
        }

        /**
         * @return whether the {@link #task} has all loop initialization input dependenchannelInstancees satisfied
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
         * @return whether the {@link #task} has all loop iteration input dependenchannelInstancees satisfied
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
