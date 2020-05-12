package org.qcri.rheem.giraph.execution;

import org.apache.giraph.conf.GiraphConfiguration;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.platform.*;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.giraph.operators.GiraphExecutionOperator;
import org.qcri.rheem.giraph.platform.GiraphPlatform;

import java.util.*;

/**
 * {@link Executor} for the {@link GiraphPlatform}.
 */
public class GiraphExecutor extends ExecutorTemplate {
    private final GiraphPlatform platform;

    private Configuration configuration;

    private Job job;

    private GiraphConfiguration giraphConfiguration;

    public GiraphExecutor(GiraphPlatform platform, Job job) {
        super(job.getCrossPlatformExecutor());
        this.job = job;
        this.platform = platform;
        this.configuration = job.getConfiguration();

        this.giraphConfiguration = new GiraphConfiguration();
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
        final GiraphExecutionOperator giraphExecutionOperator = (GiraphExecutionOperator) task.getOperator();

        ChannelInstance[] inputChannelInstances = new ChannelInstance[task.getNumInputChannels()];
        for (int i = 0; i < inputChannelInstances.length; i++) {
            inputChannelInstances[i] = executionState.getChannelInstance(task.getInputChannel(i));
        }
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.getOperatorContext(giraphExecutionOperator);
        ChannelInstance[] outputChannelInstances = new ChannelInstance[task.getNumOuputChannels()];
        for (int i = 0; i < outputChannelInstances.length; i++) {
            outputChannelInstances[i] = task.getOutputChannel(i).createInstance(this, operatorContext, i);
        }

        long startTime = System.currentTimeMillis();
        final Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> results =
                giraphExecutionOperator.execute(inputChannelInstances, outputChannelInstances, this, operatorContext);
        long endTime = System.currentTimeMillis();

        final Collection<ExecutionLineageNode> executionLineageNodes = results.getField0();
        final Collection<ChannelInstance> producedChannelInstances = results.getField1();

        for (ChannelInstance outputChannelInstance : outputChannelInstances) {
            if (outputChannelInstance != null) {
                executionState.register(outputChannelInstance);
            }
        }

        final PartialExecution partialExecution = this.createPartialExecution(executionLineageNodes, endTime - startTime);
        executionState.add(partialExecution);
        this.registerMeasuredCardinalities(producedChannelInstances);
    }

    @Override
    public void dispose() {
        // Maybe clean up some files?
    }

    @Override
    public GiraphPlatform getPlatform() {
        return this.platform;
    }

    public GiraphConfiguration getGiraphConfiguration(){
        return this.giraphConfiguration;
    }
}
