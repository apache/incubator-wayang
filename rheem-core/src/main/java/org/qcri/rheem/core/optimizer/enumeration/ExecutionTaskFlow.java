package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.plan.executionplan.*;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.traversal.AbstractTopologicalTraversal;

import java.util.*;
import java.util.stream.Stream;

/**
 * Graph of {@link ExecutionTask}s and {@link Channel}s. Does not define {@link ExecutionStage}s and
 * {@link PlatformExecution}s - in contrast to a final {@link ExecutionPlan}.
 */
public class ExecutionTaskFlow {

    private final Collection<ExecutionTask> sinkTasks;

    private final Set<Channel> inputChannels;

    public ExecutionTaskFlow(Collection<ExecutionTask> sinkTasks) {
        this(sinkTasks, Collections.emptySet());
    }

    public ExecutionTaskFlow(Collection<ExecutionTask> sinkTasks, Set<Channel> inputChannels) {
        assert !sinkTasks.isEmpty() : "Cannot build plan without sinks.";
        this.sinkTasks = sinkTasks;
        this.inputChannels = inputChannels;
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
