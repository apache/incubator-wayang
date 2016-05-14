package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.plan.executionplan.*;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.LoopHeadOperator;
import org.qcri.rheem.core.profiling.InstrumentationStrategy;
import org.qcri.rheem.core.util.AbstractReferenceCountable;
import org.qcri.rheem.core.util.Formats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
     * Maintains the {@link Executor}s for each {@link PlatformExecution}.
     */
    private final Map<PlatformExecution, Executor> executors = new HashMap<>();

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
     * Keeps track of {@link Channel} cardinalities.
     */
    private final Map<Channel, Long> cardinalities = new HashMap<>();

    /**
     * Maintains {@link ExecutionResource}s that are "global" w.r.t. to this instance, i.e., they will not be
     * instantly disposed if not currently used.
     */
    private final Set<ExecutionResource> globalResources = new HashSet<>(2);

    /**
     * Keeps track of {@link ChannelInstance}s so as to reuse them among {@link Executor} runs.
     */
    private final Map<Channel, ChannelInstance> channelInstances = new HashMap<>();

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
    public boolean executeUntilBreakpoint(ExecutionPlan executionPlan) {
        // Initialize this instance from the executionPlan.
        this.prepare(executionPlan);

        // Run until the #breakpoint inhibits or no ExecutionStages are left.
        this.runToBreakpoint();

        return this.suspendedStages.isEmpty();
    }

    /**
     * Clean up {@link ExecutionStage}-related state and re-create {@link StageActivator}s etc. from the {@link ExecutionPlan}/
     *
     * @param executionPlan whose {@link ExecutionStage}s will be executed
     */
    public void prepare(ExecutionPlan executionPlan) {
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
            final StageActivator activator = this.getOrCreateActivator(stage);
            this.tryToActivate(activator);
        }
    }

    /**
     * Returns an existing {@link StageActivator} for the {@link ExecutionStage} or creates and registers a new one.
     *
     * @param stage for which the {@link StageActivator} is requested
     * @return the {@link StageActivator}
     */
    private StageActivator getOrCreateActivator(ExecutionStage stage) {
        return this.pendingStageActivators.computeIfAbsent(stage, StageActivator::new);
    }

    /**
     * If the {@link StageActivator} can be activate, move it from {@link #pendingStageActivators} to {@link #activatedStageActivators}.
     *
     * @param activator that should be activated
     * @return whether an activation took place
     */
    private boolean tryToActivate(StageActivator activator) {
        if (activator.updateInputChannelInstances()) {
            // Activate the activator by moving it.
            this.pendingStageActivators.remove(activator.getStage());
            this.activatedStageActivators.add(activator);
            return true;
        }
        return false;
    }

    /**
     * Activate and execute {@link ExecutionStage}s as far as possible.
     */
    private void runToBreakpoint() {
        // Start execution traversal.
        final long startTime = System.currentTimeMillis();
        int numExecutedStages = 0;
        boolean isBreakpointsDisabled = false;
        do {
            // Execute and activate as long as possible.
            while (!this.activatedStageActivators.isEmpty()) {
                final StageActivator stageActivator = this.activatedStageActivators.poll();

                // Check if #breakpoint permits the execution.
                if (!isBreakpointsDisabled && this.suspendIfBreakpointRequest(stageActivator)) {
                    continue;
                }

                // Otherwise, execute the stage.
                final ExecutionStage stage = stageActivator.getStage();
                this.execute(stage);
                numExecutedStages++;

                // Try to activate the successor stages.
                this.tryToActivateSuccessors(stage);

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

            // Safety net to recover from illegal Breakpoint configurations.
            if (!isBreakpointsDisabled && numExecutedStages == 0) {
                this.logger.warn("Could not execute a single stage. Will retry with disabled breakpoints.");
                isBreakpointsDisabled = true;
                this.activatedStageActivators.addAll(this.suspendedStages);
                this.suspendedStages.clear();
            } else {
                isBreakpointsDisabled = false;
            }
        } while (!this.activatedStageActivators.isEmpty());

        final long finishTime = System.currentTimeMillis();
        CrossPlatformExecutor.this.logger.info("Executed {} stages in {}.",
                numExecutedStages, Formats.formatDuration(finishTime - startTime));

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
        if (this.breakpoint.requestsBreakBefore(stageActivator.getStage())) {
            this.suspendedStages.add(stageActivator);
            return true;
        }
        return false;
    }

    /**
     * Tries to execute the given {@link ExecutionStage}.
     *
     * @param stage that should be executed
     * @return whether the {@link ExecutionStage} was really executed
     */
    private void execute(ExecutionStage stage) {
        // Find parts of the stage to instrument.
        this.instrumentationStrategy.applyTo(stage);

        // Obtain an Executor for the stage.
        Executor executor = this.getOrCreateExecutorFor(stage);

        // Have the execution done.
        CrossPlatformExecutor.this.logger.info("Having {} execute {}:\n{}", executor, stage, stage.getPlanAsString("> "));
        long startTime = System.currentTimeMillis();
        executor.execute(stage, this);
        long finishTime = System.currentTimeMillis();
        CrossPlatformExecutor.this.logger.info("Executed {} in {}.", stage, Formats.formatDuration(finishTime - startTime));

        // Obtain instrumentation results.
//        throw new RuntimeException("todo");
//        executionState.getCardinalities().forEach((channel, cardinality) ->
//                CrossPlatformExecutor.this.logger.debug("Cardinality of {}: actual {}, estimated {}",
//                  this.executionState.merge(executionState);

        // Clean up.
        this.completedStages.add(stage);
    }

    private Executor getOrCreateExecutorFor(ExecutionStage stage) {
        return this.executors.computeIfAbsent(
                stage.getPlatformExecution(),
                pe -> {
                    // It is important to register the Executor. This way, we ensure that it will also not be disposed
                    // among disconnected PlatformExecutions. The downside is, that we only remove it, once the
                    // execution is done.
                    final Executor executor = pe.getPlatform().getExecutorFactory().create(this.job);
                    this.registerGlobal(executor);
                    return executor;
                }
        );
    }

    /**
     * Increments the {@link #predecessorCounter} for all successors of the given {@link ExecutionStage} and
     * activates them if possible by putting them in the given {@link Collection}.
     */
    private void tryToActivateSuccessors(ExecutionStage processedStage) {
        // Gather all successor ExecutionStages for that a new ChannelInstance has been produced.
        final Collection<Channel> outboundChannels = processedStage.getOutboundChannels();
        Set<ExecutionStage> successorStages = new HashSet<>(outboundChannels.size());
        for (Channel outboundChannel : outboundChannels) {
            if (this.getChannelInstance(outboundChannel) != null) {
                for (ExecutionTask consumer : outboundChannel.getConsumers()) {
                    final ExecutionStage consumerStage = consumer.getStage();
                    // We must be careful: outbound Channels still can have consumers within the producer's ExecutionStage.
                    if (consumerStage != processedStage) {
                        successorStages.add(consumerStage);
                    }
                }
            }
        }

        // Try to activate follow-up stages.
        for (ExecutionStage successorStage : successorStages) {
            final StageActivator activator = this.getOrCreateActivator(successorStage);
            this.tryToActivate(activator);
        }
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
        return this.channelInstances.get(channel);
    }

    @Override
    public void register(ChannelInstance channelInstance) {
        final ChannelInstance oldChannelInstance = this.channelInstances.put(channelInstance.getChannel(), channelInstance);
        channelInstance.noteObtainedReference();
        if (oldChannelInstance != null) {
            oldChannelInstance.noteDiscardedReference(true);
        }
    }

    /**
     * Register a global {@link ExecutionResource}, that will only be released once this instance has
     * finished executing.
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
    public Map<Channel, Long> getCardinalities() {
        return this.cardinalities;
    }

    /**
     * Set a new {@link Breakpoint} for this instance.
     *
     * @param breakpoint the new {@link Breakpoint}
     */
    public void setBreakpoint(Breakpoint breakpoint) {
        this.breakpoint = breakpoint;
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
         * @param stage that should be activated
         */
        private StageActivator(ExecutionStage stage) {
            this.stage = stage;

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
            return this.updateChannelInstances(this.miscInboundChannels) & (
                    this.updateChannelInstances(this.initializationInboundChannels) |
                            this.updateChannelInstances(this.iterationInboundChannels)
            );
        }

        /**
         * Try to satisfy the input {@link ChannelInstance} requirements for all given {@link Channel}s by updating {@link #inputChannelInstances}.
         *
         * @param channels for that the {@link ChannelInstance}s are requested
         * @return whether there are {@link ChannelInstance}s for all {@link Channel}s available
         */
        private boolean updateChannelInstances(Collection<Channel> channels) {
            boolean isAllChannelsAvailable = true;
            for (Channel channel : channels) {
                // Check if the ChannelInstance is already known.
                if (this.inputChannelInstances.containsKey(channel)) continue;

                // Otherwise, check if it is available now.
                final ChannelInstance channelInstance = CrossPlatformExecutor.this.getChannelInstance(channel);
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

        @Override
        protected void disposeUnreferenced() {
            for (ExecutionResource loopInvariant : this.loopInvariants) {
                loopInvariant.noteDiscardedReference(true);
            }
            this.loopInvariants = null;
            CrossPlatformExecutor.this.removeLoopContext(this.loop);
        }

        @Override
        public boolean isDisposed() {
            return this.loopInvariants == null;
        }
    }

}
