package org.qcri.rheem.graphchi.execution;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.ExecutionState;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.graphchi.GraphChiPlatform;
import org.qcri.rheem.graphchi.operators.GraphChiOperator;

import java.util.*;

/**
 * {@link Executor} for the GraphChiPlatform.
 */
public class GraphChiExecutor implements Executor {

    private final GraphChiPlatform platform;

    private final Configuration configuration;

    public GraphChiExecutor(GraphChiPlatform platform, Configuration configuration) {
        this.platform = platform;
        this.configuration = configuration;
    }

    @Override
    public ExecutionState execute(final ExecutionStage stage, ExecutionState executionState) {
        Queue<ExecutionTask> scheduledTasks = new LinkedList<>(stage.getStartTasks());
        Set<ExecutionTask> executedTasks = new HashSet<>();

        final ExecutionState newExecutionState = new ExecutionState();
        final Map<Channel, ChannelInstance> allChannelInstances = new HashMap<>(executionState.getChannelInstances());

        while (!scheduledTasks.isEmpty()) {
            final ExecutionTask task = scheduledTasks.poll();
            if (executedTasks.contains(task)) continue;
            this.execute(task, allChannelInstances, newExecutionState);
            executedTasks.add(task);
            Arrays.stream(task.getOutputChannels())
                    .flatMap(channel -> channel.getConsumers().stream())
                    .filter(consumer -> consumer.getStage() == stage)
                    .forEach(scheduledTasks::add);
        }

        return newExecutionState;
    }

    /**
     * Brings the given {@code task} into execution.
     */
    private void execute(ExecutionTask task, Map<Channel, ChannelInstance> allChannelInstances, ExecutionState newExecutionState) {
        ChannelInstance[] inputChannelInstances = new ChannelInstance[task.getNumInputChannels()];
        for (int i = 0; i < inputChannelInstances.length; i++) {
            inputChannelInstances[i] = allChannelInstances.get(task.getInputChannel(i));
        }
        ChannelInstance[] outputChannelInstances = new ChannelInstance[task.getNumOuputChannels()];
        for (int i = 0; i < outputChannelInstances.length; i++) {
            outputChannelInstances[i] = task.getOutputChannel(i).createInstance();
        }
        final GraphChiOperator graphChiOperator = (GraphChiOperator) task.getOperator();
        graphChiOperator.execute(inputChannelInstances, outputChannelInstances, this.configuration);
        for (ChannelInstance outputChannelInstance : outputChannelInstances) {
            if (outputChannelInstance != null) {
                newExecutionState.getChannelInstances().put(outputChannelInstance.getChannel(), outputChannelInstance);
            }
        }
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
