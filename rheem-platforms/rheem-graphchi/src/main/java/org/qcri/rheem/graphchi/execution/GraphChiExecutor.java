package org.qcri.rheem.graphchi.execution;

import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.platform.ExecutionProfile;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.graphchi.GraphChiPlatform;
import org.qcri.rheem.graphchi.operators.GraphChiOperator;

import java.util.*;

/**
 * {@link Executor} for the GraphChiPlatform.
 */
public class GraphChiExecutor implements Executor {

    private final GraphChiPlatform platform;

    public GraphChiExecutor(GraphChiPlatform platform) {
        this.platform = platform;
    }

    @Override
    public ExecutionProfile execute(final ExecutionStage stage) {
        Queue<ExecutionTask> scheduledTasks = new LinkedList<>(stage.getStartTasks());
        Set<ExecutionTask> executedTasks = new HashSet<>();

        while (!scheduledTasks.isEmpty()) {
            final ExecutionTask task = scheduledTasks.poll();
            if (executedTasks.contains(task)) continue;
            ;
            this.execute(task);
            executedTasks.add(task);
            Arrays.stream(task.getOutputChannels())
                    .flatMap(channel -> channel.getConsumers().stream())
                    .filter(consumer -> consumer.getStage() == stage)
                    .forEach(scheduledTasks::add);
        }

        return new ExecutionProfile();
    }

    /**
     * Brings the given {@code task} into execution.
     */
    private void execute(ExecutionTask task) {
        Channel[] inputChannels = Arrays.copyOfRange(task.getInputChannels(), 0, task.getOperator().getNumInputs());
        Channel[] outputChannels = Arrays.copyOfRange(task.getOutputChannels(), 0, task.getOperator().getNumOutputs());
        final GraphChiOperator graphChiOperator = (GraphChiOperator) task.getOperator();
        graphChiOperator.execute(inputChannels, outputChannels);
    }

    @Override
    public void dispose() {
        // Maybe clean up some files?
    }

    @Override
    public GraphChiPlatform getPlatform() {
        return this.platform;
    }
}
