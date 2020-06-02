package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionStageLoop;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.LoopHeadOperator;
import org.qcri.rheem.core.plan.rheemplan.LoopSubplan;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.profiling.InstrumentationStrategy;
import org.qcri.rheem.core.util.AbstractReferenceCountable;
import org.qcri.rheem.core.util.Formats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Executes a (cross-platform) {@link ExecutionPlan}.
 */
public class CrossPlatformExecutor implements ExecutionState {

    public final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The {@link Job} that is being executed by this instance.
     */
    private final Job job;

    /**
     * Aggregates user-defined {@link Breakpoint}s. Will be cleared after each execution.
     */
    private Breakpoint breakpoint = Breakpoint.NONE;

    /**
     * All {@link ExecutionStage}s in the processed {@link ExecutionPlan}.
     */
    private final Set<ExecutionStage> allStages = new HashSet<>();

    /**
     * Activated and considered for execution.
     */
    private final Queue<StageActivator> activatedStageActivators = new LinkedList<>();

    /**
     * Keeps track of {@link StageActivator}s.
     */
    private final Map<ExecutionStage, StageActivator> pendingStageActivators = new HashMap<>();

    /**
     * Maintains the {@link Executor}s for each {@link Platform}.
     */
    private final Map<Platform, Executor> executors = new HashMap<>();

    /**
     * We keep them around if we want to go on without re-optimization.
     */
    private final Collection<StageActivator> suspendedStages = new LinkedList<>();

    /**
     * When executing an {@link ExecutionStageLoop}, we might need to reuse several {@link ExecutionResource}s
     * among all iterations. If we would go with our normal handling scheme, we might lose them after the first
     * iteration. Therefore, we actively keep track of them via {@link ExecutionStageLoopContext}s.
     */
    private final Map<ExecutionStageLoop, ExecutionStageLoopContext> loopContexts = new HashMap<>();

    /**
     * Marks {@link Channel}s for instrumentation.
     */
    private final InstrumentationStrategy instrumentationStrategy;

    /**
     * Keeps track of {@link ExecutionStage}s that have actually been executed by this instance.
     */
    private Set<ExecutionStage> completedStages = new HashSet<>();

    /**
     * Keeps track of {@link ChannelInstance} cardinalities.
     */
    private final Collection<ChannelInstance> cardinalityMeasurements = new LinkedList<>();

    /**
     * Maintains {@link ExecutionResource}s that are "global" w.r.t. to this instance, i.e., they will not be
     * instantly disposed if not currently used.
     */
    private final Set<ExecutionResource> globalResources = new HashSet<>(2);

    /**
     * Keeps track of {@link ChannelInstance}s so as to reuse them among {@link Executor} runs.
     */
    private final Map<Channel, ChannelInstance> channelInstances = new HashMap<>();

    /**
     * Gathers {@link PartialExecution}s created during the execution.
     */
    private final Collection<PartialExecution> partialExecutions = new LinkedList<>();

    /**
     * Gathers {@link ParallelExecutionThread}s created during parallel execution.
     */
    private final ArrayList<Thread> parallelExecutionThreads = new ArrayList<>();

    /**
     * Keeps track of the completed {@link ParallelExecutionThread}s created during parallel execution.
     */
    private volatile int completedThreads;

    public CrossPlatformExecutor(Job job, InstrumentationStrategy instrumentationStrategy) {
        this.job = job;
        this.instrumentationStrategy = instrumentationStrategy;
    }

    /**
     * Execute the given {@link ExecutionPlan}.
     *
     * @param executionPlan that should be executed or continued
     * @return whether the {@link ExecutionPlan} was completed (i.e., not suspended due to the {@link Breakpoint})
     */
    public boolean executeUntilBreakpoint(ExecutionPlan executionPlan, OptimizationContext optimizationContext) {
        // Initialize this instance from the executionPlan.
        this.prepare(executionPlan, optimizationContext);

        // Run until the #breakpoint inhibits or no ExecutionStages are left.
        this.runToBreakpoint();

        return this.suspendedStages.isEmpty();
    }

    /**
     * Clean up {@link ExecutionStage}-related state and re-create {@link StageActivator}s etc. from the {@link ExecutionPlan}/
     *
     * @param executionPlan       whose {@link ExecutionStage}s will be executed
     * @param optimizationContext contains additional optimization info for the {@code executionPlan}
     */
    public void prepare(ExecutionPlan executionPlan, OptimizationContext optimizationContext) {
        this.allStages.clear();
        this.activatedStageActivators.clear();
        this.suspendedStages.clear();

        // Remove obsolete StageActivators (after re-optimization).
        this.allStages.addAll(executionPlan.getStages());
        new ArrayList<>(this.pendingStageActivators.keySet()).stream()
                .filter(stage -> !this.allStages.contains(stage))
                .forEach(this.pendingStageActivators::remove);

        // Create StageActivators for all ExecutionStages.
        for (ExecutionStage stage : this.allStages) {
            // Avoid re-activating already executed ExecutionStages.
            if (this.completedStages.contains(stage)) continue;
            final StageActivator activator = this.getOrCreateActivator(
                    stage,
                    () -> this.determineInitialOptimizationContext(stage, optimizationContext)
            );
            this.tryToActivate(activator);
        }
    }

    /**
     * Find the initial {@link OptimizationContext} for a {@link StageActivator}.
     *
     * @param stage the {@link ExecutionStage} whose {@link OptimizationContext} is to be determined
     * @return the {@link OptimizationContext}
     */
    private OptimizationContext determineInitialOptimizationContext(ExecutionStage stage, OptimizationContext rootOptimizationContext) {
        if (stage.getLoop() == null) return rootOptimizationContext;

        // TODO: Assumes non-nested loops.
        return rootOptimizationContext.getNestedLoopContext(stage.getLoop().getLoopSubplan()).getInitialIterationContext();
    }

    /**
     * Returns an existing {@link StageActivator} for the {@link ExecutionStage} or creates and registers a new one.
     *
     * @param stage                       for which the {@link StageActivator} is requested
     * @param optimizationContextSupplier supplies an {@link OptimizationContext} for the {@code stage}
     * @return the {@link StageActivator}
     */
    private StageActivator getOrCreateActivator(
            ExecutionStage stage,
            final Supplier<OptimizationContext> optimizationContextSupplier
    ) {
        return this.pendingStageActivators.computeIfAbsent(stage, s -> new StageActivator(s, optimizationContextSupplier.get()));
    }

    /**
     * If the {@link StageActivator} can be activate, move it from {@link #pendingStageActivators} to {@link #activatedStageActivators}.
     *
     * @param activator that should be activated
     * @return whether an activation took place
     */
    private boolean tryToActivate(StageActivator activator) {
        if (activator.updateInputChannelInstances()) {
            logger.info("Activating {}.", activator.getStage());
            // Activate the activator by moving it.
            this.pendingStageActivators.remove(activator.getStage());
            assert this.activatedStageActivators.stream().noneMatch(a -> a.getStage().equals(activator.getStage())) :
                    String.format("Must not activate %s twice.", activator.getStage());
            this.activatedStageActivators.add(activator);
            activator.noteActivation();
            return true;
        }
        return false;
    }

    /**
     * Execute one single {@link ExecutionStage}
     */

    private void executeSingleStage(boolean isBreakpointsDisabled, StageActivator stageActivator) {
        // Check if #breakpoint permits the execution.
        if (!isBreakpointsDisabled && this.suspendIfBreakpointRequest(stageActivator)) {
            return;
        }

        // Otherwise, execute the stage.
        this.execute(stageActivator);

        // Try to activate the successor stages.
        this.tryToActivateSuccessors(stageActivator);

        // We can now dispose the stageActivator that collected the input ChannelInstances.
        stageActivator.dispose();

        // Dispose obsolete ChannelInstances.
        final Iterator<Map.Entry<Channel, ChannelInstance>> iterator = this.channelInstances.entrySet().iterator();
        while (iterator.hasNext()) {
            final Map.Entry<Channel, ChannelInstance> channelInstanceEntry = iterator.next();
            final ChannelInstance channelInstance = channelInstanceEntry.getValue();

            // If this is instance is the only one to still use this ChannelInstance, discard it.
            if (channelInstance.getNumReferences() == 1) {
                channelInstance.noteDiscardedReference(true);
                iterator.remove();
            }
        }
    }


    /**
     * Run parallel threads executing activated {@link ExecutionStage}s
     */
    private void runParallelExecution(boolean isBreakpointsDisabled) {
        int numActiveStages = this.activatedStageActivators.size();

        // Create execution threads
        for (int i = 1; i <= numActiveStages; ++i) {
            // TODO: Better pass the stage to the thread rather than letting the thread retrieve the stage itself (to avoid concurrency issues).
            Thread thread = new Thread(new ParallelExecutionThread(isBreakpointsDisabled, "T" + String.valueOf(i), this));
            // Start thread execution
            thread.start();
            this.parallelExecutionThreads.add(thread);
        }

        //Join all created threads
        for (Thread t : this.parallelExecutionThreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                CrossPlatformExecutor.this.logger.error("Thread Interrupted!", e);
            }
        }

        // Clear the list of created threads
        parallelExecutionThreads.clear();
        CrossPlatformExecutor.this.logger.info("Parallel execution ended!");
    }

    /**
     * Activate and execute {@link ExecutionStage}s as far as possible.
     */
    private void runToBreakpoint() {
        // Start execution traversal.
        final long startTime = System.currentTimeMillis();
        int numPriorExecutedStages = this.completedStages.size();
        int numExecutedStages;
        boolean isBreakpointsDisabled = false;
        do {
            // Execute and activate as long as possible.
            while (!this.activatedStageActivators.isEmpty()) {
                // Check if there is multiple activated stages to start parallelization
                if (this.activatedStageActivators.size() > 1 &&
                        this.getConfiguration().getBooleanProperty("rheem.core.optimizer.enumeration.parallel-tasks")) {
                    // Run multiple threads for each independant stage
                    this.runParallelExecution(isBreakpointsDisabled);
                } else {
                    final StageActivator stageActivator = this.activatedStageActivators.poll();
                    // Execute one single ExecutionStage
                    this.executeSingleStage(isBreakpointsDisabled, stageActivator);
                }
            }

            // Safety net to recover from illegal Breakpoint configurations.
            numExecutedStages = this.completedStages.size() - numPriorExecutedStages;
            if (!isBreakpointsDisabled && numExecutedStages == 0) {
                this.logger.warn("Could not execute a single stage. Will retry with disabled breakpoints.");
                isBreakpointsDisabled = true;
                this.activatedStageActivators.addAll(this.suspendedStages);
                this.suspendedStages.clear();
            } else {
                isBreakpointsDisabled = false;
            }
        } while (!this.activatedStageActivators.isEmpty());

        // Get the number of executed stages in current runToBreakpoint
        final long finishTime = System.currentTimeMillis();
        CrossPlatformExecutor.this.logger.info("Executed {} stages in {}.",
                numExecutedStages, Formats.formatDuration(finishTime - startTime, true));

        assert numExecutedStages > 0 : "Did not execute a single stage.";
    }

    /**
     * If the {@link #breakpoint} requests not to execute the given {@link ExecutionStage}, put it to
     * {@link #suspendedStages}.
     *
     * @param stageActivator that might be suspended
     * @return whether the {@link ExecutionStage} was suspended
     */
    private boolean suspendIfBreakpointRequest(StageActivator stageActivator) {
        if (!this.breakpoint.permitsExecutionOf(stageActivator.getStage(), this, this.job.getOptimizationContext())) {
            this.suspendedStages.add(stageActivator);
            return true;
        }
        return false;
    }

    /**
     * Tries to execute the given {@link ExecutionStage}.
     *
     * @param stageActivator that should be executed
     * @return whether the {@link ExecutionStage} was really executed
     */
    private void execute(StageActivator stageActivator) {
        final ExecutionStage stage = stageActivator.getStage();
        final OptimizationContext optimizationContext = stageActivator.getOptimizationContext();

        // Find parts of the stage to instrument.
        this.instrumentationStrategy.applyTo(stage);

        // Obtain an Executor for the stage.
        Executor executor = this.getOrCreateExecutorFor(stage);

        // Have the execution done.
        CrossPlatformExecutor.this.logger.info("Having {} execute {}:\n{}", executor, stage, stage.getPlanAsString("> "));
        long startTime = System.currentTimeMillis();
        executor.execute(stage, optimizationContext, this);
        long finishTime = System.currentTimeMillis();
        CrossPlatformExecutor.this.logger.info("Executed {} in {}.", stage, Formats.formatDuration(finishTime - startTime, true));

        // Remember that we have executed the stage.
        this.completedStages.add(stage);

        if (stage.isLoopHead()) {
            this.getOrCreateLoopContext(stage.getLoop()).scrapPreviousTransitionContext();
        }
    }

    private Executor getOrCreateExecutorFor(ExecutionStage stage) {
        return this.executors.computeIfAbsent(
                stage.getPlatformExecution().getPlatform(),
                platform -> {
                    // It is important to register the Executor. This way, we ensure that it will also not be disposed
                    // among disconnected PlatformExecutions. The downside is, that we only remove it, once the
                    // execution is done.
                    final Executor executor = platform.getExecutorFactory().create(this.job);
                    this.registerGlobal(executor);
                    return executor;
                }
        );
    }

    /**
     * Follows the outbound {@link Channel}s of the given {@link ExecutionStage} and try to activate consuming
     * {@link ExecutionStage}s, given the according {@link ChannelInstance}s are available.
     *
     * @param processedStageActivator should have just been executed
     */
    private void tryToActivateSuccessors(StageActivator processedStageActivator) {
        final ExecutionStage processedStage = processedStageActivator.getStage();

        // Gather all successor ExecutionStages for that a new ChannelInstance has been produced.
        final Collection<Channel> outboundChannels = processedStage.getOutboundChannels();
        Set<ExecutionStage> successorStages = new HashSet<>(outboundChannels.size());
        for (Channel outboundChannel : outboundChannels) {
            for (ExecutionTask consumer : outboundChannel.getConsumers()) {
                if (this.getChannelInstance(outboundChannel, consumer.isFeedbackInput(outboundChannel)) != null) {
                    final ExecutionStage consumerStage = consumer.getStage();
                    // We must be careful: outbound Channels still can have consumers within the producer's ExecutionStage.
                    if (consumerStage != processedStage && !consumerStage.isInFinishedLoop()) {
                        successorStages.add(consumerStage);
                    }
                }
            }
        }

        // Try to activate follow-up stages.
        for (ExecutionStage successorStage : successorStages) {
            final StageActivator activator = this.getOrCreateActivator(
                    successorStage,
                    () -> this.determineNextOptimizationContext(processedStageActivator, successorStage)
            );
            this.tryToActivate(activator);
        }
    }

    /**
     * Find the {@link OptimizationContext} for a {@link StageActivator} that is activated from a preceeding
     * {@link StageActivator}.
     *
     * @param processedStageActivator the preceeding {@link StageActivator}
     * @param successorStage          the {@link StageActivator} whose {@link OptimizationContext} is to be determined
     * @return the {@link OptimizationContext}
     */
    private OptimizationContext determineNextOptimizationContext(
            StageActivator processedStageActivator,
            ExecutionStage successorStage
    ) {
        final OptimizationContext prevOptimizationContext = processedStageActivator.getOptimizationContext();

        // If the sucessor is not in a loop, we go to the root OptimizationContext.
        if (successorStage.getLoop() == null) {
            return prevOptimizationContext.getRootParent();
        }

        final ExecutionStage processedStage = processedStageActivator.getStage();
        if (processedStage.getLoop() == successorStage.getLoop()) {
            // If we stay in the very same loop...
            if (successorStage.isLoopHead()) {
                // ...and the next stage is a loop head, then we need to switch to the next iteration context.
                return prevOptimizationContext.getNextIterationContext();
            } else {
                // We need to check, that we do not run into the last iteration context, where there is no more
                // information for the loop body.
                if (!prevOptimizationContext.isFinalIteration()) {
                    return prevOptimizationContext;
                }
                // Sneak in a new OptimizationContext for the new iteration.
                return prevOptimizationContext.getLoopContext().appendIterationContext();

            }
        }

        // Otherwise, we enter a loop.
        // TODO: This code assumes non-nested loops.
        final LoopSubplan loopSubplan = successorStage.getLoop().getLoopSubplan();
        return prevOptimizationContext
                .getRootParent()
                .getNestedLoopContext(loopSubplan)
                .getInitialIterationContext();
    }


    /**
     * Retrieve an existing {@link ExecutionStageLoopContext} or create a new one for a {@link ExecutionStageLoop}.
     *
     * @param loop for that a {@link ExecutionStageLoopContext} is requested
     * @return the {@link ExecutionStageLoopContext}
     */
    private ExecutionStageLoopContext getOrCreateLoopContext(ExecutionStageLoop loop) {
        return this.loopContexts.computeIfAbsent(loop, ExecutionStageLoopContext::new);
    }

    /**
     * Removes an {@link ExecutionStageLoopContext}.
     *
     * @param loop that is described by the {@link ExecutionStageLoopContext}
     */
    private void removeLoopContext(ExecutionStageLoop loop) {
        final ExecutionStageLoopContext context = this.loopContexts.remove(loop);
        assert context.getNumReferences() == 0;
    }

    @Override
    public ChannelInstance getChannelInstance(Channel channel) {
        return this.getChannelInstance(channel, false);
    }

    public ChannelInstance getChannelInstance(Channel channel, boolean isPeekingToNextTransition) {
        final ExecutionStageLoop loop = getExecutionStageLoop(channel);
        if (loop == null) {
            return this.channelInstances.get(channel);
        } else {
            final ExecutionStageLoopContext loopContext = this.getOrCreateLoopContext(loop);
            return loopContext.getChannelInstance(channel, isPeekingToNextTransition);
        }
    }

    /**
     * Determine the {@link ExecutionStageLoop} the given {@link Channel} belongs to.
     *
     * @param channel the {@link Channel}
     * @return the {@link ExecutionStageLoop} or {@code null} if none
     */
    private static ExecutionStageLoop getExecutionStageLoop(Channel channel) {
        final ExecutionStage producerStage = channel.getProducer().getStage();
        if (producerStage.getLoop() == null) return null;
        final OutputSlot<?> output = channel.getProducer().getOutputSlotFor(channel);
        if (output != null
                && output.getOwner().isLoopHead()
                && ((LoopHeadOperator) output.getOwner()).getFinalLoopOutputs().contains(output)) {
            return null;
        }
        return producerStage.getLoop();
    }

    @Override
    public void register(ChannelInstance channelInstance) {
        final ExecutionStageLoop loop = getExecutionStageLoop(channelInstance.getChannel());
        if (loop == null) {
            final ChannelInstance oldChannelInstance = this.channelInstances.put(channelInstance.getChannel(), channelInstance);
            channelInstance.noteObtainedReference();
            if (oldChannelInstance != null) {
                oldChannelInstance.noteDiscardedReference(true);
            }
        } else {
            final ExecutionStageLoopContext loopContext = this.getOrCreateLoopContext(loop);
            loopContext.register(channelInstance);
        }
    }

    /**
     * Register a global {@link ExecutionResource}, that will only be released once this instance has
     * finished executing.
     *
     * @param resource that should be registered
     */
    public void registerGlobal(ExecutionResource resource) {
        if (this.globalResources.add(resource)) {
            resource.noteObtainedReference();
        } else {
            this.logger.warn("Registered {} twice.", resource);
        }
    }

    @Override
    public void addCardinalityMeasurement(ChannelInstance channelInstance) {
        this.cardinalityMeasurements.add(channelInstance);
    }

    @Override
    public Collection<ChannelInstance> getCardinalityMeasurements() {
        return this.cardinalityMeasurements;
    }

    @Override
    public void add(PartialExecution partialExecution) {
        this.partialExecutions.add(partialExecution);
        if (this.logger.isInfoEnabled()) {
            this.logger.info(
                    "Executed {} items in {} (estimated {}).",
                    partialExecution.getAtomicExecutionGroups().size(),
                    Formats.formatDuration(partialExecution.getMeasuredExecutionTime()),
                    partialExecution.getOverallTimeEstimate(this.getConfiguration())
            );
        }
    }

    @Override
    public Collection<PartialExecution> getPartialExecutions() {
        return this.partialExecutions;
    }

    /**
     * Set a new {@link Breakpoint} for this instance.
     *
     * @param breakpoint the new {@link Breakpoint}
     */
    public void setBreakpoint(Breakpoint breakpoint) {
        this.breakpoint = breakpoint;
    }

    /**
     * Allows to inhibit changes to the {@link ExecutionPlan}, such as on re-optimization.
     *
     * @return whether this instance is vetoing on changes
     */
    public boolean isVetoingPlanChanges() {
        return !this.loopContexts.isEmpty();
    }

    public void shutdown() {
        // Release global resources.
        this.globalResources.forEach(resource -> resource.noteDiscardedReference(true));
        this.globalResources.clear();
    }

    /**
     * Retrieve the {@link ExecutionStage}s that have been completed so far.
     *
     * @return the completed {@link ExecutionStage}s
     */
    public Set<ExecutionStage> getCompletedStages() {
        return this.completedStages;
    }

    public Configuration getConfiguration() {
        return this.job.getConfiguration();
    }

    /**
     * Observes the {@link CrossPlatformExecutor} execution state in order to tell when the input dependencies of
     * a {@link ExecutionStage} are satisfied so that it can be activated.
     */
    private class StageActivator {

        /**
         * The {@link ExecutionStage} being activated.
         */
        private final ExecutionStage stage;

        /**
         * The {@link OptimizationContext} for the {@link #stage}.
         */
        private final OptimizationContext optimizationContext;

        /**
         * Regular inbound {@link Channel}s to the {@link #stage}.
         */
        private final Collection<Channel> miscInboundChannels = new LinkedList<>();

        /**
         * Inbound {@link Channel}s to the {@link #stage}, that implement an initialization {@link InputSlot}
         * of a {@link LoopHeadOperator}.
         */
        private final Collection<Channel> initializationInboundChannels = new LinkedList<>();

        /**
         * Inbound {@link Channel}s to the {@link #stage}, that implement an iteration {@link InputSlot}
         * of a {@link LoopHeadOperator}.
         */
        private final Collection<Channel> iterationInboundChannels = new LinkedList<>();

        /**
         * Inbound {@link Channel}s to the {@link #stage}, that are loop invariant, i.e., that are needed across
         * several iteration. Of course, this is only possible if {@link #stage} is in an {@link ExecutionStageLoop}.
         */
        private final Collection<Channel> loopInvariantInboundChannels = new LinkedList<>();

        /**
         * If the {@link #stage} is part of a {@link ExecutionStageLoop}, we need to keep track of an associated
         * runtime {@link ExecutionStageLoopContext} that manages loop invariant {@link ExecutionResource}s. In particular,
         * this instance is a stake-holder of this {@link ExecutionStageLoopContext} and tells that it must not be
         * removed.
         */
        private final ExecutionStageLoopContext loopContext;

        /**
         * Keep track of the {@link ChannelInstance}s that are inputs of the {@link #stage}.
         */
        private final Map<Channel, ChannelInstance> inputChannelInstances = new HashMap<>(4);

        /**
         * Creates a new instance.
         *
         * @param stage               that should be activated
         * @param optimizationContext
         */
        private StageActivator(ExecutionStage stage, OptimizationContext optimizationContext) {
            this.stage = stage;
            this.optimizationContext = optimizationContext;

            if (this.stage.getLoop() != null) {
                this.loopContext = CrossPlatformExecutor.this.getOrCreateLoopContext(this.stage.getLoop());
                this.loopContext.noteObtainedReference();
            } else {
                this.loopContext = null;
            }

            // Distinguish the inbound Channels of the stage.
            final Collection<Channel> inboundChannels = this.stage.getInboundChannels();
            if (this.stage.isLoopHead()) {
                assert this.stage.getAllTasks().size() == 1 : String.format("Loop head stage looks like this:\n%s", this.stage.getPlanAsString("! "));

                // Loop heads are special in the sense that they don't require all of their inputs.
                for (Channel inboundChannel : inboundChannels) {
                    for (ExecutionTask executionTask : inboundChannel.getConsumers()) {
                        // Avoid consumers from other ExecutionStages.
                        if (executionTask.getStage() != this.stage) continue;

                        // Check special inputs with iteration semantics.
                        if (executionTask.getOperator().isLoopHead()) {
                            LoopHeadOperator loopHead = (LoopHeadOperator) executionTask.getOperator();
                            final InputSlot<?> targetInput = executionTask.getInputSlotFor(inboundChannel);
                            if (loopHead.getLoopBodyInputs().contains(targetInput)) {
                                this.iterationInboundChannels.add(inboundChannel);
                                continue;
                            } else if (loopHead.getLoopInitializationInputs().contains(targetInput)) {
                                this.initializationInboundChannels.add(inboundChannel);
                                continue;
                            }
                        }

                        // Otherwise, we treat it as a regular inbound Channel.
                        this.miscInboundChannels.add(inboundChannel);
                        assert this.checkIfIsLoopInput(inboundChannel) :
                                String.format("%s is not a loop input as expected.", inboundChannel);
                        this.loopInvariantInboundChannels.add(inboundChannel);
                    }
                }

            } else {
                // If we do not have a loop head, we treat all Channels as regular ones.
                this.miscInboundChannels.addAll(inboundChannels);

                // Still, the Channels might be loop inputs.
                for (Channel inboundChannel : inboundChannels) {
                    if (this.checkIfIsLoopInput(inboundChannel)) {
                        this.loopInvariantInboundChannels.add(inboundChannel);
                    }
                }
            }
        }

        /**
         * Checks if the given {@link Channel} (inbound to {@link #stage}), is entering an {@link ExecutionStageLoop}
         * when serving the {@link #stage}.
         *
         * @param inboundChannel {@link Channel} that is inbound to {@link #stage} and that might be a {@link ExecutionStageLoop} input
         * @return whether the {@link Channel} is a {@link ExecutionStageLoop} input, i.e., it is not produced in an {@link ExecutionStageLoop}
         * while this {@link #stage} is part of a {@link ExecutionStageLoop}
         */
        private boolean checkIfIsLoopInput(Channel inboundChannel) {
            // NB: This code assumes no nested loops.
            return this.stage.getLoop() != null && this.stage.getLoop() != inboundChannel.getProducer().getStage().getLoop();
        }

        /**
         * Try to satisfy the input {@link ChannelInstance} requirements by updating {@link #inputChannelInstances}.
         *
         * @return whether the activation is possible
         */
        boolean updateInputChannelInstances() {
            boolean isMiscChannelsReady = this.updateChannelInstances(this.miscInboundChannels, false);
            boolean isLoopChannelsReady = true;
            if (this.stage.isLoopHead()) {
                LoopHeadOperator loopOperator = (LoopHeadOperator) this.stage.getLoopHeadTask().getOperator();
                switch (loopOperator.getState()) {
                    case NOT_STARTED:
                        isLoopChannelsReady = this.updateChannelInstances(this.initializationInboundChannels, false);
                        break;
                    case RUNNING:
                        isLoopChannelsReady = this.updateChannelInstances(this.iterationInboundChannels, true);
                        break;
                    default:
                        logger.warn("Tried to update input channel instances for finished {}.", this.stage);
                        isLoopChannelsReady = false;
                }
            }
            return isMiscChannelsReady && isLoopChannelsReady;
        }

        /**
         * Try to satisfy the input {@link ChannelInstance} requirements for all given {@link Channel}s by updating {@link #inputChannelInstances}.
         *
         * @param channels for that the {@link ChannelInstance}s are requested
         * @return whether there are {@link ChannelInstance}s for all {@link Channel}s available
         */
        private boolean updateChannelInstances(Collection<Channel> channels, boolean isFeedback) {
            boolean isAllChannelsAvailable = true;
            for (Channel channel : channels) {
                // Check if the ChannelInstance is already known.
                if (this.inputChannelInstances.containsKey(channel)) continue;

                // Otherwise, check if it is available now.
                final ChannelInstance channelInstance = CrossPlatformExecutor.this.getChannelInstance(channel, isFeedback);
                if (channelInstance != null) {
                    // If so, reference it.
                    this.inputChannelInstances.put(channel, channelInstance);
                    channelInstance.noteObtainedReference();

                    // Also, check if this is a loop invariant input.
                    if (this.loopInvariantInboundChannels.contains(channel)) {
                        this.loopContext.registerLoopInvariant(channelInstance);
                    }
                } else {
                    // Otherwise, we will have a negative result. Still, we need to grab all Channels to save them from disposal.
                    isAllChannelsAvailable = false;
                }
            }
            return isAllChannelsAvailable;
        }

        /**
         * Getter for the {@link ExecutionStage} activated by this instance.
         *
         * @return the {@link ExecutionStage}
         */
        public ExecutionStage getStage() {
            return this.stage;
        }

        public OptimizationContext getOptimizationContext() {
            return optimizationContext;
        }

        /**
         * Dispose this instance, in particular discard references to its {@link ChannelInstance}s.
         */
        void dispose() {
            // Release the inputChannelInstances.
            for (ChannelInstance channelInstance : this.inputChannelInstances.values()) {
                channelInstance.noteDiscardedReference(true);
            }
            this.inputChannelInstances.clear();

            // Release the loopContext.
            if (this.loopContext != null) {
                this.loopContext.noteDiscardedReference(true);
            }
        }

        /**
         * Notifies this instance that it has been activated.
         */
        public void noteActivation() {
            if (this.stage.isLoopHead()) this.loopContext.activateNextIteration();
        }
    }


    /**
     * Keeps track of {@link ExecutionResource}s of an {@link ExecutionStageLoop}.
     */
    private class ExecutionStageLoopContext extends AbstractReferenceCountable {

        /**
         * The {@link ExecutionStageLoop} being described by this instance.
         */
        private final ExecutionStageLoop loop;

        /**
         * Maintains the loop invariant {@link ExecutionResource}s.
         */
        private Set<ExecutionResource> loopInvariants = new HashSet<>(4);

        private ExecutionStageLoopIterationContext currentIteration, prevTransition, nextTransition;

        /**
         * Creates a new instance.
         *
         * @param executionStageLoop is described by the new instance
         */
        public ExecutionStageLoopContext(ExecutionStageLoop executionStageLoop) {
            this.loop = executionStageLoop;
        }

        /**
         * Registers a loop invariant {@link ExecutionResource} with this instance.
         *
         * @param loopInvariant the said {@link ExecutionResource}
         */
        void registerLoopInvariant(ExecutionResource loopInvariant) {
            if (this.loopInvariants.add(loopInvariant)) {
                loopInvariant.noteObtainedReference();
            }
        }

        /**
         * Switch the state of this instance: Age the next to the previous transition and create a new
         * current {@link ExecutionStageLoopIterationContext}.
         */
        public void activateNextIteration() {
            logger.info("Activating next iteration.");
            if (this.currentIteration != null) this.currentIteration.noteDiscardedReference(true);
            this.currentIteration = this.createIterationContext();

            if (this.prevTransition != null) this.prevTransition.noteDiscardedReference(true);
            this.prevTransition = this.nextTransition;
            this.nextTransition = null;
        }

        private ExecutionStageLoopIterationContext createIterationContext() {
            final ExecutionStageLoopIterationContext iteration = new ExecutionStageLoopIterationContext(this);
            iteration.noteObtainedReference();
            return iteration;
        }

        /**
         * Create a new {@link ExecutionStageLoopIterationContext} for the next transition if it does not exist.
         *
         * @return the {@link ExecutionStageLoopIterationContext} for the next transition
         */
        public ExecutionStageLoopIterationContext getOrCreateNextTransition() {
            if (this.nextTransition == null) {
                this.nextTransition = this.createIterationContext();
            }
            return this.nextTransition;
        }

        @Override
        protected void disposeUnreferenced() {
            for (ExecutionResource loopInvariant : this.loopInvariants) {
                loopInvariant.noteDiscardedReference(true);
            }
            this.loopInvariants = null;
            CrossPlatformExecutor.this.removeLoopContext(this.loop);
            if (this.prevTransition != null) this.prevTransition.noteDiscardedReference(true);
            if (this.currentIteration != null) this.currentIteration.noteDiscardedReference(true);
            if (this.nextTransition != null) this.nextTransition.noteDiscardedReference(true);
        }

        @Override
        public boolean isDisposed() {
            return this.loopInvariants == null;
        }

        /**
         * Registers the given {@link ChannelInstance} with appropriate {@link ExecutionStageLoopIterationContext}s.
         *
         * @param channelInstance the {@link ChannelInstance}
         */
        public void register(ChannelInstance channelInstance) {
            final Channel channel = channelInstance.getChannel();
            boolean isFeedback = false, isIterationLocal = false;
            for (ExecutionTask consumer : channel.getConsumers()) {
                if (consumer.isFeedbackInput(channel)) {
                    isFeedback = true;
                } else {
                    isIterationLocal = true;
                }
            }
            if (isIterationLocal) this.currentIteration.register(channelInstance);
            if (isFeedback) this.getOrCreateNextTransition().register(channelInstance);
        }

        /**
         * Provide a {@link ChannelInstance} from this instance. The instance can be provided from the previous or
         * next transition or the current {@link ExecutionStageLoopIterationContext}.
         *
         * @param channel                   whose {@link ChannelInstance} should be provided
         * @param isPeekingToNextTransition whether the next transition {@link ExecutionStageLoopIterationContext}
         *                                  may be accessed
         * @return the {@link ChannelInstance} or {@code null} if it cannot be found
         */
        public ChannelInstance getChannelInstance(Channel channel,
                                                  boolean isPeekingToNextTransition) {
            if (isPeekingToNextTransition) {
                return this.getOrCreateNextTransition().getChannelInstance(channel);
            }

            if (this.prevTransition != null) {
                final ChannelInstance channelInstance = this.prevTransition.getChannelInstance(channel);
                if (channelInstance != null) return channelInstance;
            }

            if (this.currentIteration != null) {
                return this.currentIteration.getChannelInstance(channel);
            }
            return null;
        }

        /**
         * Removes the previous transition {@link ExecutionStageLoopIterationContext}. Included resources
         * will not be provided anymore by this instance.
         */
        public void scrapPreviousTransitionContext() {
            if (this.prevTransition != null) this.prevTransition.noteDiscardedReference(true);
            this.prevTransition = null;
        }
    }

    /**
     * Keeps track of {@link ExecutionResource}s of an {@link ExecutionStageLoop}.
     */
    private class ExecutionStageLoopIterationContext extends AbstractReferenceCountable {

        /**
         * The hosting {@link ExecutionStageLoopContext}.
         */
        private final ExecutionStageLoopContext loopContext;

        /**
         * Maintains {@link ChannelInstance}s produced in this iteration.
         */
        private Map<Channel, ChannelInstance> channelInstances = new HashMap<>(8);

        /**
         * Creates a new instance.
         */
        private ExecutionStageLoopIterationContext(ExecutionStageLoopContext loopContext) {
            this.loopContext = loopContext;
        }

        @Override
        protected void disposeUnreferenced() {
            for (ChannelInstance channelInstance : channelInstances.values()) {
                channelInstance.noteDiscardedReference(true);
            }
            this.channelInstances = null;
        }

        /**
         * Registers the given {@link ChannelInstance} with this instance.
         *
         * @param channelInstance that should be registered
         */
        public void register(ChannelInstance channelInstance) {
            channelInstance.noteObtainedReference();
            final ChannelInstance oldInstance = this.channelInstances.put(channelInstance.getChannel(), channelInstance);
            if (oldInstance != null) oldInstance.noteDiscardedReference(true);
        }

        public ChannelInstance getChannelInstance(Channel channel) {
            return this.channelInstances.get(channel);
        }
    }

    /**
     * Executes {@link ExecutionStage}s in parallel threads
     * It continues to live as long as there is a {@link ExecutionStage} activated after first {@link ExecutionStage} execution and another running {@link ParallelExecutionThread}s,
     * if multiple {@link ExecutionStage} are activated it will create new threads to execute new {@link ExecutionStage} in recursive manner
     */

    private class ParallelExecutionThread implements Runnable {

        /**
         * Thread identifier of {@link ParallelExecutionThread}
         */
        public String threadId;

        /**
         * Check if #breakpoint permits the execution of {@link ExecutionStage}
         */
        private boolean thread_isBreakpointDisabled;

        /**
         * {@link CrossPlatformExecutor} initiating the running thread
         */
        private final CrossPlatformExecutor crossPlatformExecutor;

        /**
         * Creates a new instance.
         */
        public ParallelExecutionThread(boolean isBreakpointsDisabled, String id, CrossPlatformExecutor cpe) {
            this.thread_isBreakpointDisabled = isBreakpointsDisabled;
            this.threadId = id;
            this.crossPlatformExecutor = cpe;
        }

        /**
         * Execution code of the thread.
         */
        @Override
        public void run() {

            StageActivator stageActivator;
            this.crossPlatformExecutor.logger.info("Thread " + String.valueOf(this.threadId) + " started");
            // Loop until there is no activated stage or only one thread running
            do {
                // Get the stageActivator for the stage to execute
                synchronized (this.crossPlatformExecutor) {
                    stageActivator = this.crossPlatformExecutor.activatedStageActivators.poll();
                    if (stageActivator == null)
                        break;
                }
                this.crossPlatformExecutor.logger.info(this.threadId + " started executing Stage: {}:", stageActivator.getStage());

                // Check if #breakpoint permits the execution.
                if (!this.thread_isBreakpointDisabled && this.crossPlatformExecutor.suspendIfBreakpointRequest(stageActivator)) {
                    return;
                }

                // Otherwise, execute the stage.

                final ExecutionStage stage = stageActivator.getStage();
                final OptimizationContext optimizationContext = stageActivator.getOptimizationContext();

                // Find parts of the stage to instrument.
                this.crossPlatformExecutor.instrumentationStrategy.applyTo(stage);

                // Obtain an Executor for the stage.
                final Executor executor = this.crossPlatformExecutor.getOrCreateExecutorFor(stage);

                // Have the execution done.
                CrossPlatformExecutor.this.logger.info("Having {} execute {}:\n{}", executor, stage, stage.getPlanAsString("> "));
                long startTime = System.currentTimeMillis();

                // synchronize(this.crossplateform) can be used here to avoid error when we have two stages running same operators even on the same platform but still with different executors
                synchronized (executor) {
                    executor.execute(stage, optimizationContext, this.crossPlatformExecutor);
                    long finishTime = System.currentTimeMillis();

                    CrossPlatformExecutor.this.logger.info("Executed {} in {}.", stage, Formats.formatDuration(finishTime - startTime, true));

                    // Remember that we have executed the stage.
                    this.crossPlatformExecutor.completedStages.add(stage);
                    if (stage.isLoopHead()) {
                        this.crossPlatformExecutor.getOrCreateLoopContext(stage.getLoop()).scrapPreviousTransitionContext();
                    }

                    // Try to activate the successor stages.
                    this.crossPlatformExecutor.tryToActivateSuccessors(stageActivator);
                }
                // We can now dispose the stageActivator that collected the input ChannelInstances.
                stageActivator.dispose();

                // Dispose obsolete ChannelInstances.
                final Iterator<Map.Entry<Channel, ChannelInstance>> iterator = this.crossPlatformExecutor.channelInstances.entrySet().iterator();
                while (iterator.hasNext()) {
                    final Map.Entry<Channel, ChannelInstance> channelInstanceEntry = iterator.next();
                    final ChannelInstance channelInstance = channelInstanceEntry.getValue();

                    // If this is instance is the only one to still use this ChannelInstance, discard it.
                    if (channelInstance.getNumReferences() == 1) {
                        channelInstance.noteDiscardedReference(true);
                        iterator.remove();
                    }

                }

                this.crossPlatformExecutor.logger.info(this.threadId + " completed executing Stage : {}:", stageActivator.getStage());

                // Create new threads for more than one activated stages recursively
                if (CrossPlatformExecutor.this.activatedStageActivators.size() > 1) {
                    // Create new threads other than the existing thread
                    for (int i = 1; i <= CrossPlatformExecutor.this.activatedStageActivators.size() - 1; i++) {
                        // TODO: Better use Java's ForkJoinPool to reduce thread creation overhead and control concurrency.
                        // Create parallel stage execution thread
                        Thread thread = new Thread(new ParallelExecutionThread(this.thread_isBreakpointDisabled, "T" + String.valueOf(i) + "@" + this.threadId, this.crossPlatformExecutor));
                        thread.start();
                        synchronized (this.crossPlatformExecutor) {
                            //Add the created thread to {@link #parallelExecutionThreads}
                            CrossPlatformExecutor.this.parallelExecutionThreads.add(thread);
                        }
                    }
                }

            }

            // TODO: Do not busy-wait (could be solved with the ForkJoinPool as well).
            while (CrossPlatformExecutor.this.activatedStageActivators.size() >= 1 &&
                    CrossPlatformExecutor.this.parallelExecutionThreads.size() - CrossPlatformExecutor.this.completedThreads > 1);

            // Increment a global variable of completed threads
            // As long as the variable is volatile so there is no concern of race condition
            CrossPlatformExecutor.this.completedThreads++;

            // Notify thread ended
            CrossPlatformExecutor.this.logger.info(this.threadId + " ended");
        }
    }

}
