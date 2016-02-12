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

    /**
     * Execute the given {@link ExecutionPlan}.
     */
    public void execute(ExecutionPlan executionPlan) {
        new Execution(executionPlan).run();
    }

    /**
     * Encapsulates a single execution of an {@link ExecutionPlan}.
     */
    private class Execution {

        private Map<ExecutionStage, Integer> numCompletedPredecessors = new HashMap<>();

        private Queue<ExecutionStage> activatedStages = new LinkedList<>();

        private Map<PlatformExecution, Executor> executors = new HashMap<>();

        private Map<PlatformExecution, Integer> executedStages = new HashMap<>();

        private Execution(ExecutionPlan executionPlan) {
            this.activatedStages.addAll(executionPlan.getStartingStages());
        }

        public void run() {
            final long startTime = System.currentTimeMillis();
            int numExecutedStages = 0;
            Collection<ExecutionStage> newlyActivatedStages = new LinkedList<>();
            do {
                while (!this.activatedStages.isEmpty()) {
                    final ExecutionStage nextStage = this.activatedStages.poll();
                    Executor executor = this.getExecutor(nextStage);
                    this.execute(nextStage, executor);
                    numExecutedStages++;
                    this.disposeExecutorIfDone(nextStage.getPlatformExecution(), executor);
                    this.tryToActivateSuccessors(nextStage, newlyActivatedStages);
                }
                this.activatedStages.addAll(newlyActivatedStages);
                newlyActivatedStages.clear();
            } while (!this.activatedStages.isEmpty());
            final long finishTime = System.currentTimeMillis();
            CrossPlatformExecutor.this.logger.info("Executed {} stages in {}.",
                    numExecutedStages, Formats.formatDuration(finishTime - startTime));
        }

        private Executor getExecutor(ExecutionStage nextStage) {
            return this.executors.computeIfAbsent(
                    nextStage.getPlatformExecution(),
                    pe -> pe.getPlatform().getExecutorFactory().create()
            );
        }

        private void execute(ExecutionStage stage, Executor executor) {
            long startTime = System.currentTimeMillis();
            executor.execute(stage);
            long finishTime = System.currentTimeMillis();
            CrossPlatformExecutor.this.logger.info("Executed {} in {}.", stage, Formats.formatDuration(finishTime - startTime));
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

}
