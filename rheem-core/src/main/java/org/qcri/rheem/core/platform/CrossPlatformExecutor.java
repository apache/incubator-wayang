package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.PlatformExecution;
import org.qcri.rheem.core.util.Counter;
import org.qcri.rheem.core.util.Formats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Executes a (cross-platform) {@link ExecutionPlan}.
 */
public class CrossPlatformExecutor {

    public final Logger logger = LoggerFactory.getLogger(this.getClass());

    private Breakpoint breakpoint;

    /**
     * Number of completed predecessors per {@link ExecutionStage}.
     */
    private Counter<ExecutionStage> predecessorCounter = new Counter<>();

    /**
     * Activated and considered for execution.
     */
    private Queue<ExecutionStage> activatedStages = new LinkedList<>();

    /**
     * Maintains the {@link Executor}s for each {@link PlatformExecution}.
     */
    private Map<PlatformExecution, Executor> executors = new HashMap<>();

    /**
     * The number of executed {@link ExecutionStage}s per {@link PlatformExecution}.
     */
    private Counter<PlatformExecution> executionStageCounter = new Counter<>();

    private Set<ExecutionStage> completedStages = new HashSet<>();

    /**
     * We keep them around if we want to go on without re-optimization.
     */
    private Collection<ExecutionStage> suspendedStages = new LinkedList<>();

    private ExecutionProfile executionProfile;

    /**
     * Execute the given {@link ExecutionPlan}.
     */
    public State executeUntilBreakpoint(ExecutionPlan executionPlan) {
        this.prepare(executionPlan);
        this.runToBreakpoint();
        this.setBreakpoint(null);
        return this.captureState();
    }

    /**
     * Prepare this instance to fast-forward already executed parts of the new {@link ExecutionPlan}.
     */
    public void prepare(ExecutionPlan executionPlan) {
        this.predecessorCounter.clear();
        this.activatedStages.clear();
        this.activatedStages.addAll(executionPlan.getStartingStages());
        this.executionProfile = new ExecutionProfile();
    }

    private void runToBreakpoint() {
        // Start execution traversal.
        final long startTime = System.currentTimeMillis();
        int numExecutedStages = 0;
        Collection<ExecutionStage> newlyActivatedStages = new LinkedList<>();
        do {
            while (!this.activatedStages.isEmpty()) {
                final ExecutionStage nextStage = this.activatedStages.poll();

                // Check if #breakpoint permits the execution.
                if (this.suspendIfBreakpointRequest(nextStage)) continue;

                // Otherwise, execute the stage.
                if (this.execute(nextStage)) {
                    numExecutedStages++;
                }
                this.tryToActivateSuccessors(nextStage, newlyActivatedStages);

            }
            this.activatedStages.addAll(newlyActivatedStages);
            newlyActivatedStages.clear();
        } while (!this.activatedStages.isEmpty());
        final long finishTime = System.currentTimeMillis();
        CrossPlatformExecutor.this.logger.info("Executed {} stages in {}.",
                numExecutedStages, Formats.formatDuration(finishTime - startTime));
    }


    private boolean suspendIfBreakpointRequest(ExecutionStage activatedStage) {
        if (CrossPlatformExecutor.this.breakpoint != null &&
                !CrossPlatformExecutor.this.breakpoint.permitsExecutionOf(activatedStage)) {
            this.suspendedStages.add(activatedStage);
            return true;
        }
        return false;
    }

    /**
     * Tries to execute the given {@link ExecutionStage}.
     *
     * @return whether the {@link ExecutionStage} was really executed
     */
    private boolean execute(ExecutionStage activatedStage) {
        if (!this.completedStages.contains(activatedStage)) {
            Executor executor = this.getOrCreateExecutorFor(activatedStage);
            final ExecutionProfile executionProfile = this.submit(activatedStage, executor);
            this.executionProfile.merge(executionProfile);
            this.completedStages.add(activatedStage);
            this.disposeExecutorIfDone(activatedStage.getPlatformExecution(), executor);
            return true;

        } else {
            CrossPlatformExecutor.this.logger.info("Skipping already executed {}.", activatedStage);
            return false;
        }
    }

    private Executor getOrCreateExecutorFor(ExecutionStage stage) {
        return this.executors.computeIfAbsent(
                stage.getPlatformExecution(),
                pe -> pe.getPlatform().getExecutorFactory().create()
        );
    }

    /**
     * Submit the {@code stage} to the {@code executor}.
     *
     * @return the {@link ExecutionProfile} that has been gathered during execution
     */
    private ExecutionProfile submit(ExecutionStage stage, Executor executor) {
        CrossPlatformExecutor.this.logger.info("Start executing {}.", stage);
        long startTime = System.currentTimeMillis();
        final ExecutionProfile executionProfile = executor.execute(stage);
        long finishTime = System.currentTimeMillis();
        CrossPlatformExecutor.this.logger.info("Executed {} in {}.", stage, Formats.formatDuration(finishTime - startTime));
        executionProfile.getCardinalities().forEach((channel, cardinality) ->
                CrossPlatformExecutor.this.logger.info("Cardinality of {}: actual {}, estimated {}",
                        channel, cardinality, channel.getCardinalityEstimate())
        );
        return executionProfile;
    }

    /**
     * Increments the {@link #executionStageCounter} for the given {@link PlatformExecution} and disposes
     * {@link Executor}s if it is no longer needed.
     */
    private void disposeExecutorIfDone(PlatformExecution platformExecution, Executor executor) {
        int numExecutedStages = this.executionStageCounter.increment(platformExecution);
        if (numExecutedStages == platformExecution.getStages().size()) {
            executor.dispose();
            this.executors.remove(platformExecution);
        }
    }

    /**
     * Increments the {@link #predecessorCounter} for all successors of the given {@link ExecutionStage} and
     * activates them if possible by putting them in the given {@link Collection}.
     */
    private void tryToActivateSuccessors(ExecutionStage processedStage, Collection<ExecutionStage> collector) {
        for (ExecutionStage succeedingStage : processedStage.getSuccessors()) {
            final int newCompletedPredecessors = this.predecessorCounter.increment(succeedingStage);
            if (newCompletedPredecessors == succeedingStage.getPredecessors().size()) {
                collector.add(succeedingStage);
                this.predecessorCounter.remove(succeedingStage);
            }
        }
    }

    public void setBreakpoint(Breakpoint breakpoint) {
        this.breakpoint = breakpoint;
    }

    /**
     * @return the internal state of this instance
     */
    public State captureState() {
        return new State(this);
    }

    /**
     * Intermediate state of an interrupted execution of an {@link ExecutionPlan}.
     */
    public static class State {

        private final ExecutionProfile profile = new ExecutionProfile();

        private final Set<ExecutionStage> completedStages = new HashSet<>(), suspendedStages = new HashSet<>();

        public State(CrossPlatformExecutor crossPlatformExecutor) {
            this.profile.merge(crossPlatformExecutor.executionProfile);
            this.completedStages.addAll(crossPlatformExecutor.completedStages);
            this.suspendedStages.addAll(crossPlatformExecutor.suspendedStages);
        }

        public ExecutionProfile getProfile() {
            return this.profile;
        }

        public Set<ExecutionStage> getCompletedStages() {
            return this.completedStages;
        }

        public Set<ExecutionStage> getSuspendedStages() {
            return this.suspendedStages;
        }

        public boolean isComplete() {
            return this.suspendedStages.isEmpty();
        }
    }


}
