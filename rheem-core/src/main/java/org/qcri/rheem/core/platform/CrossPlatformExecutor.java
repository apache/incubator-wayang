package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.executionplan.*;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.LoopHeadOperator;
import org.qcri.rheem.core.profiling.InstrumentationStrategy;
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
    private final Queue<ExecutionStage> activatedStages = new LinkedList<>();

    /**
     * Keeps track of {@link StageActivator}s.
     */
    private final Map<ExecutionStage, StageActivator> stageActivators = new HashMap<>();

    /**
     * Maintains the {@link Executor}s for each {@link PlatformExecution}.
     */
    private final Map<PlatformExecution, Executor> executors = new HashMap<>();

    /**
     * We keep them around if we want to go on without re-optimization.
     */
    private final Collection<ExecutionStage> suspendedStages = new LinkedList<>();

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
        this.activatedStages.clear();
        this.suspendedStages.clear();

        // Remove obsolete StageActivators (after re-optimization).
        this.allStages.addAll(executionPlan.getStages());
        new ArrayList<>(this.stageActivators.keySet()).stream()
                .filter(stage -> !this.allStages.contains(stage))
                .forEach(this.stageActivators::remove);

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
        return this.stageActivators.computeIfAbsent(stage, StageActivator::new);
    }

    /**
     * If the {@link StageActivator} can be activate, move it from {@link #stageActivators} to {@link #activatedStages}.
     *
     * @param activator that should be activated
     * @return whether an activation took place
     */
    private boolean tryToActivate(StageActivator activator) {
        if (activator.canBeActivated()) {
            this.activatedStages.add(activator.getStage());
            this.stageActivators.remove(activator.getStage());
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
            while (!this.activatedStages.isEmpty()) {
                final ExecutionStage nextStage = this.activatedStages.poll();

                // Check if #breakpoint permits the execution.
                if (!isBreakpointsDisabled && this.suspendIfBreakpointRequest(nextStage)) {
                    continue;
                }

                // Otherwise, execute the stage.
                this.execute(nextStage);
                numExecutedStages++;

                // Try to activate the successor stages.
                this.tryToActivateSuccessors(nextStage);
            }

            // Safety net to recover from illegal Breakpoint configurations.
            if (!isBreakpointsDisabled && numExecutedStages == 0) {
                this.logger.warn("Could not execute a single stage. Will retry with disabled breakpoints.");
                isBreakpointsDisabled = true;
                this.activatedStages.addAll(this.suspendedStages);
                this.suspendedStages.clear();
            } else {
                isBreakpointsDisabled = false;
            }
        } while (!this.activatedStages.isEmpty());

        final long finishTime = System.currentTimeMillis();
        CrossPlatformExecutor.this.logger.info("Executed {} stages in {}.",
                numExecutedStages, Formats.formatDuration(finishTime - startTime));

        // Sanity check. TODO: Should be an assertion, shouldn't it?
        if (numExecutedStages == 0) {
            throw new RheemException("Could not execute a single stage. Are the Rheem plan and breakpoints correct?");
        }
    }

    /**
     * If the {@link #breakpoint} requests not to execute the given {@link ExecutionStage}, put it to
     * {@link #suspendedStages}.
     *
     * @param activatedStage that might be suspended
     * @return whether the {@link ExecutionStage} was suspended
     */
    private boolean suspendIfBreakpointRequest(ExecutionStage activatedStage) {
        if (this.breakpoint.requestsBreakBefore(activatedStage)) {
            this.suspendedStages.add(activatedStage);
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
        CrossPlatformExecutor.this.logger.info("Start executing {}.", stage);
        CrossPlatformExecutor.this.logger.info("Stage plan:\n{}", stage.toExtensiveString());
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
        this.disposeExecutorIfDone(stage.getPlatformExecution());
    }

    private Executor getOrCreateExecutorFor(ExecutionStage stage) {
        return this.executors.computeIfAbsent(
                stage.getPlatformExecution(),
                pe -> pe.getPlatform().getExecutorFactory().create(this.job)
        );
    }

    /**
     * Increments the {@link #executionStageCounter} for the given {@link PlatformExecution} and disposes
     * {@link Executor}s if it is no longer needed.
     */
    private void disposeExecutorIfDone(PlatformExecution platformExecution) {
        // TODO
//        int numExecutedStages = this.executionStageCounter.increment(platformExecution);
//        if (numExecutedStages == platformExecution.getStages().size()) {
//            // Be cautious: In replay mode, we do not want to re-summon the Executor.
//            final Executor executor = this.executors.get(platformExecution);
//            if (executor != null) {
//                executor.dispose();
//                this.executors.remove(platformExecution);
//            }
//        }
    }

    /**
     * Increments the {@link #predecessorCounter} for all successors of the given {@link ExecutionStage} and
     * activates them if possible by putting them in the given {@link Collection}.
     */
    private void tryToActivateSuccessors(ExecutionStage processedStage) {
        for (ExecutionStage succeedingStage : processedStage.getSuccessors()) {
            final Collection<Channel> inboundChannels = succeedingStage.getInboundChannels();
            if (this.channelInstances.keySet().stream().anyMatch(inboundChannels::contains)) {
                final StageActivator activator = this.getOrCreateActivator(succeedingStage);
                this.tryToActivate(activator);
            }
        }
    }

    @Override
    public ChannelInstance getChannelInstance(Channel channel) {
        return this.channelInstances.get(channel);
    }

    @Override
    public void register(ChannelInstance channelInstance) {
        this.channelInstances.put(channelInstance.getChannel(), channelInstance);
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
        this.executors.values().forEach(Executor::dispose);
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

        private final Collection<Channel> miscInboundChannels = new LinkedList<>();

        private final Collection<Channel> initializationInboundChannels = new LinkedList<>();

        private final Collection<Channel> iterationInboundChannels = new LinkedList<>();

        /**
         * Creates a new instance.
         *
         * @param stage that should be activated
         */
        private StageActivator(ExecutionStage stage) {
            this.stage = stage;

            // Distinguish the inbound Channels of the stage.
            final Collection<Channel> inboundChannels = this.stage.getInboundChannels();
            if (this.stage.isLoopHead()) {
                // Loop heads are special in the sense that they don't require all of their inputs.
                for (Channel inboundChannel : inboundChannels) {
                    for (ExecutionTask executionTask : inboundChannel.getConsumers()) {
                        if (executionTask.getStage() != this.stage) continue;
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
                        this.miscInboundChannels.add(inboundChannel);
                    }
                }
            } else {
                this.miscInboundChannels.addAll(inboundChannels);
            }
        }

        /**
         * Tests whether the {@link #stage} can be activated.
         *
         * @return whether the activation is possible
         */
        boolean canBeActivated() {
            return this.checkChannelAvailability(this.miscInboundChannels) && (
                    this.checkChannelAvailability(this.initializationInboundChannels) ||
                            this.checkChannelAvailability(this.iterationInboundChannels)
            );
        }

        /**
         * Checks whether {@link ChannelInstance}s are available for all given {@link Channel}s.
         *
         * @param channels for that the {@link ChannelInstance}s are requested
         * @return whether there are {@link ChannelInstance}s for all {@link Channel}s available
         */
        private boolean checkChannelAvailability(Collection<Channel> channels) {
            for (Channel channel : channels) {
                if (!CrossPlatformExecutor.this.channelInstances.containsKey(channel)) {
                    return false;
                }
            }
            return true;
        }

        public ExecutionStage getStage() {
            return this.stage;
        }
    }


}
