package org.qcri.rheem.graphchi.execution;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.platform.*;
import org.qcri.rheem.graphchi.platform.GraphChiPlatform;
import org.qcri.rheem.graphchi.operators.GraphChiExecutionOperator;

import java.util.*;

/**
 * {@link Executor} for the {@link GraphChiPlatform}.
 */
public class GraphChiExecutor extends ExecutorTemplate {

    private final GraphChiPlatform platform;

    private final Configuration configuration;

    public GraphChiExecutor(GraphChiPlatform platform, Job job) {
        super(job == null ? null : job.getCrossPlatformExecutor());
        this.platform = platform;
        this.configuration = job.getConfiguration();
    }

    @Override
    public void execute(final ExecutionStage stage, OptimizationContext optimizationContext, ExecutionState executionState) {
        Queue<ExecutionTask> scheduledTasks = new LinkedList<>(stage.getStartTasks());
        Set<ExecutionTask> executedTasks = new HashSet<>();

        while (!scheduledTasks.isEmpty()) {
            final ExecutionTask task = scheduledTasks.poll();
            if (executedTasks.contains(task)) continue;
            this.execute(task, optimizationContext, executionState);
            executedTasks.add(task);
            Arrays.stream(task.getOutputChannels())
                    .flatMap(channel -> channel.getConsumers().stream())
                    .filter(consumer -> consumer.getStage() == stage)
                    .forEach(scheduledTasks::add);
        }
    }

    /**
     * Brings the given {@code task} into execution.
     */
    private void execute(ExecutionTask task, OptimizationContext optimizationContext, ExecutionState executionState) {
        final GraphChiExecutionOperator graphChiExecutionOperator = (GraphChiExecutionOperator) task.getOperator();

        ChannelInstance[] inputChannelInstances = new ChannelInstance[task.getNumInputChannels()];
        for (int i = 0; i < inputChannelInstances.length; i++) {
            inputChannelInstances[i] = executionState.getChannelInstance(task.getInputChannel(i));
        }
        ChannelInstance[] outputChannelInstances = new ChannelInstance[task.getNumOuputChannels()];
        for (int i = 0; i < outputChannelInstances.length; i++) {
            outputChannelInstances[i] = task
                    .getOutputChannel(i)
                    .createInstance(this, optimizationContext.getOperatorContext(graphChiExecutionOperator), i);
        }
        graphChiExecutionOperator.execute(inputChannelInstances, outputChannelInstances, this.configuration);
        for (ChannelInstance outputChannelInstance : outputChannelInstances) {
            if (outputChannelInstance != null) {
                executionState.register(outputChannelInstance);
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
