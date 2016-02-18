package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.PlatformExecution;
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
     * Execute the given {@link ExecutionPlan}.
     */
    public State executeUntilBreakpoint(ExecutionPlan executionPlan) {
        final Execution execution = new Execution(executionPlan);
        execution.run();
        this.setBreakpoint(null);
        return execution.state;
    }

    public void setBreakpoint(Breakpoint breakpoint) {
        this.breakpoint = breakpoint;
    }

    /**
     * Encapsulates a single execution of an {@link ExecutionPlan}.
     */
    private class Execution {

        private final State state;

        private Map<ExecutionStage, Integer> numCompletedPredecessors = new HashMap<>();

        /**
         * Activated and considered for execution.
         */
        private Queue<ExecutionStage> activatedStages = new LinkedList<>();

        /**
         * Activated but the {@link #breakpoint} does not permit their execution.
         */
        private Queue<ExecutionStage> suspendedStages = new LinkedList<>();

        private Map<PlatformExecution, Executor> executors = new HashMap<>();

        private Map<PlatformExecution, Integer> executedStages = new HashMap<>();

        private Execution(ExecutionPlan executionPlan) {
            this.state = new State(executionPlan);
            this.activatedStages.addAll(executionPlan.getStartingStages());
        }

        public void run() {
            // Schedule suspended stages.
            this.rescheduleSuspendedStages();

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
                    this.execute(newlyActivatedStages, nextStage);
                    numExecutedStages++;
                }
                this.activatedStages.addAll(newlyActivatedStages);
                newlyActivatedStages.clear();
            } while (!this.activatedStages.isEmpty());
            final long finishTime = System.currentTimeMillis();
            CrossPlatformExecutor.this.logger.info("Executed {} stages in {}.",
                    numExecutedStages, Formats.formatDuration(finishTime - startTime));
        }

        private void rescheduleSuspendedStages() {
            this.activatedStages.addAll(this.suspendedStages);
            this.suspendedStages.clear();
        }

        private boolean suspendIfBreakpointRequest(ExecutionStage nextStage) {
            if (CrossPlatformExecutor.this.breakpoint != null &&
                    !CrossPlatformExecutor.this.breakpoint.permitsExecutionOf(nextStage)) {
                this.suspendedStages.add(nextStage);
                this.state.getSuspendedStages().add(nextStage);
                return true;
            }
            return false;
        }

        private void execute(Collection<ExecutionStage> newlyActivatedStages, ExecutionStage nextStage) {
            Executor executor = this.getExecutor(nextStage);
            final ExecutionProfile executionProfile = this.submit(nextStage, executor);
            this.state.getProfile().merge(executionProfile);
            this.state.getCompletedStages().add(nextStage);
            this.disposeExecutorIfDone(nextStage.getPlatformExecution(), executor);
            this.tryToActivateSuccessors(nextStage, newlyActivatedStages);
        }

        private Executor getExecutor(ExecutionStage nextStage) {
            return this.executors.computeIfAbsent(
                    nextStage.getPlatformExecution(),
                    pe -> pe.getPlatform().getExecutorFactory().create()
            );
        }

        private ExecutionProfile submit(ExecutionStage stage, Executor executor) {
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

        private void disposeExecutorIfDone(PlatformExecution platformExecution, Executor executor) {
            int numExecutedStages = this.executedStages.merge(platformExecution, 1, (count1, count2) -> count1 + count2);
            if (numExecutedStages == platformExecution.getStages().size()) {
                executor.dispose();
                this.executors.remove(platformExecution);
            }
        }

        private void tryToActivateSuccessors(ExecutionStage processedStage, Collection<ExecutionStage> newlyActivatedStages) {
            for (ExecutionStage succeedingStage : processedStage.getSuccessors()) {
                final int newCompletedPredecessors =
                        this.numCompletedPredecessors.merge(succeedingStage, 1, (count1, count2) -> count1 + count2);
                if (newCompletedPredecessors == succeedingStage.getPredecessors().size()) {
                    newlyActivatedStages.add(succeedingStage);
                    this.numCompletedPredecessors.remove(succeedingStage);
                }
            }
        }

    }

    /**
     * Intermediate state of an interrupted execution of an {@link ExecutionPlan}.
     */
    public static class State {

        private final ExecutionPlan executionPlan;

        private final ExecutionProfile profile = new ExecutionProfile();

        private final Set<ExecutionStage> completedStages = new HashSet<>(), suspendedStages = new HashSet<>();

        public State(ExecutionPlan executionPlan) {
            this.executionPlan = executionPlan;
        }

        public ExecutionPlan getExecutionPlan() {
            return this.executionPlan;
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
