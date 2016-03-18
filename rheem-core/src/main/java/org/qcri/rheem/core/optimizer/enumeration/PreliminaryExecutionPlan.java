package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.executionplan.*;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Stream;

/**
 * Graph of {@link ExecutionTask}s and {@link Channel}s. Does not define {@link ExecutionStage}s and
 * {@link PlatformExecution}s - in contrast to a final {@link ExecutionPlan}.
 */
public class PreliminaryExecutionPlan {

    private final Collection<ExecutionTask> sinkTasks;

    private final Set<Channel> inputChannels;

    private TimeEstimate timeEstimate;

    private OptimizationContext optimizationContext;

    public PreliminaryExecutionPlan(Collection<ExecutionTask> sinkTasks) {
        this(sinkTasks, Collections.emptySet());
    }

    public PreliminaryExecutionPlan(Collection<ExecutionTask> sinkTasks, Set<Channel> inputChannels) {
        assert !sinkTasks.isEmpty() : "Cannot build plan without sinks.";
        this.sinkTasks = sinkTasks;
        this.inputChannels = inputChannels;
    }

    public TimeEstimate estimateExecutionTime(Configuration configuration) {
        if (this.timeEstimate == null) {
            this.timeEstimate = this.collectAllTasks().stream()
                    .map(task -> this.getOrCalculateTimeEstimateFor(task, configuration))
                    .reduce(TimeEstimate::plus)
                    .orElseThrow(() -> new IllegalStateException("No time estimate found."));
        }
        return this.timeEstimate;
    }

    public Set<ExecutionTask> collectAllTasks() {
        Set<ExecutionTask> collector = new HashSet<>();
        this.collectAllTasksAux(this.sinkTasks.stream(), collector);
        return collector;
    }

    private void collectAllTasksAux(Stream<ExecutionTask> currentTasks, Set<ExecutionTask> collector) {
        currentTasks.forEach(task -> this.collectAllTasksAux(task, collector));
    }


    private void collectAllTasksAux(ExecutionTask currentTask, Set<ExecutionTask> collector) {
        if (collector.add(currentTask)) {
            final Stream<ExecutionTask> producerStream = Arrays.stream(currentTask.getInputChannels())
                    .filter(Objects::nonNull)
                    .map(Channel::getProducer)
                    .filter(Objects::nonNull);
            this.collectAllTasksAux(producerStream, collector);
        }
    }

    // TODO: !!!
    private TimeEstimate getOrCalculateTimeEstimateFor(ExecutionTask task, Configuration configuration) {
        final ExecutionOperator operator = task.getOperator();

        // Calculate and chache the TimeEstimate if it does not exist yet.
        if (this.optimizationContext != null) {
            final OptimizationContext.OperatorContext opCtx = this.optimizationContext.getOperatorContext(operator);
            if (opCtx != null) {
                return opCtx.getTimeEstimate();
            }
        }

        LoggerFactory.getLogger(this.getClass()).error("No time estimate for {}.", operator);
        return new TimeEstimate(100, 100000, 0.1d);
    }

    public TimeEstimate getTimeEstimate() {
        return this.timeEstimate;
    }

    public ExecutionPlan toExecutionPlan(StageAssignmentTraversal.StageSplittingCriterion... splittingCriteria) {
        return new StageAssignmentTraversal(this, splittingCriteria).run();
    }

    public boolean isComplete() {
        final Set<ExecutionTask> allTasks = this.collectAllTasks();
        if (allTasks.isEmpty()) {
            return false;
        }
        for (ExecutionTask task : allTasks) {
            if (Arrays.stream(task.getOutputChannels()).anyMatch(Objects::isNull)) {
                return false;
            }
            if (Arrays.stream(task.getInputChannels()).anyMatch(Objects::isNull)) {
                return false;
            }
        }
        return true;
    }

    public Collection<ExecutionTask> getSinkTasks() {
        return this.sinkTasks;
    }

    public Set<Channel> getInputChannels() {
        return this.inputChannels;
    }
}
