package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.executionplan.PlatformExecution;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.traversal.AbstractTopologicalTraversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Graph of {@link ExecutionTask}s and {@link Channel}s. Does not define {@link ExecutionStage}s and
 * {@link PlatformExecution}s - in contrast to a final {@link ExecutionPlan}.
 */
public class ExecutionTaskFlow {

    private final Collection<ExecutionTask> sinkTasks;

    public ExecutionTaskFlow(Collection<ExecutionTask> sinkTasks) {
        this(sinkTasks, Collections.emptySet());
    }

    @Deprecated
    public ExecutionTaskFlow(Collection<ExecutionTask> sinkTasks, Set<Channel> inputChannels) {
        assert !sinkTasks.isEmpty() : "Cannot build plan without sinks.";
        this.sinkTasks = sinkTasks;
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

    public boolean isComplete() {
        final Logger logger = LoggerFactory.getLogger(this.getClass());
        final Set<ExecutionTask> allTasks = this.collectAllTasks();
        if (allTasks.isEmpty()) {
            logger.warn("Instance has not tasks.");
            return false;
        }
        for (ExecutionTask task : allTasks) {
            if (Arrays.stream(task.getOutputChannels()).anyMatch(Objects::isNull)) {
                logger.warn("{} has missing output channels among {}.", task, Arrays.toString(task.getOutputChannels()));
                return false;
            }
            if (Arrays.stream(task.getInputChannels()).anyMatch(Objects::isNull)) {
                logger.warn("{} has missing input channels among {}.", task, Arrays.toString(task.getInputChannels()));
                return false;
            }
        }
        return true;
    }

    public Collection<ExecutionTask> getSinkTasks() {
        return this.sinkTasks;
    }

    public static ExecutionTaskFlow createFrom(PlanImplementation planImplementation) {
        final List<ExecutionOperator> startOperators = planImplementation.getStartOperators();
        assert !startOperators.isEmpty() :
                String.format("Could not find start operators among %s.", planImplementation.getStartOperators());
        final ExecutionTaskFlowCompiler executionTaskFlowCompiler = new ExecutionTaskFlowCompiler(startOperators, planImplementation);
        if (executionTaskFlowCompiler.traverse()) {
            return new ExecutionTaskFlow(executionTaskFlowCompiler.getTerminalTasks());
        } else {
            return null;
        }
    }

    public static ExecutionTaskFlow recreateFrom(PlanImplementation planImplementation,
                                                 ExecutionPlan oldExecutionPlan,
                                                 Set<Channel> openChannels,
                                                 Set<ExecutionStage> completedStages) {
        final List<ExecutionOperator> startOperators = planImplementation.getStartOperators();
        assert !startOperators.isEmpty() :
                String.format("Could not find start operators among %s.", planImplementation.getStartOperators());
        try {
            final ExecutionTaskFlowCompiler compiler = new ExecutionTaskFlowCompiler(
                    startOperators, planImplementation, oldExecutionPlan, openChannels, completedStages);
            if (compiler.traverse()) {
                return new ExecutionTaskFlow(compiler.getTerminalTasks(), compiler.getInputChannels());
            }
        } catch (AbstractTopologicalTraversal.AbortException e) {
            throw new RuntimeException(e);
        }
        return null;

    }
}
