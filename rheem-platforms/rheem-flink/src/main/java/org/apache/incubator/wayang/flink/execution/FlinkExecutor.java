package io.rheem.rheem.flink.execution;

import org.apache.flink.api.java.ExecutionEnvironment;
import io.rheem.rheem.core.api.Job;
import io.rheem.rheem.core.api.exception.RheemException;
import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.plan.executionplan.ExecutionTask;
import io.rheem.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.core.platform.Executor;
import io.rheem.rheem.core.platform.PartialExecution;
import io.rheem.rheem.core.platform.Platform;
import io.rheem.rheem.core.platform.PushExecutorTemplate;
import io.rheem.rheem.core.platform.lineage.ExecutionLineageNode;
import io.rheem.rheem.core.util.Formats;
import io.rheem.rheem.core.util.Tuple;
import io.rheem.rheem.flink.compiler.FunctionCompiler;
import io.rheem.rheem.flink.operators.FlinkExecutionOperator;
import io.rheem.rheem.flink.platform.FlinkPlatform;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * {@link Executor} implementation for the {@link FlinkPlatform}.
 */
public class FlinkExecutor extends PushExecutorTemplate {
    /**
     * Reference to a {@link ExecutionEnvironment} to be used by this instance.
     */
    private FlinkContextReference flinkContextReference;

    /**
     * The {@link ExecutionEnvironment} to be used by this instance.
     *
     */
    public ExecutionEnvironment fee;

    /**
     * Compiler to create flink UDFs.
     */
    public FunctionCompiler compiler = new FunctionCompiler();

    /**
     * Reference to the {@link ExecutionEnvironment} that provides the FlinkContextReference.
     */
    private FlinkPlatform platform;

    /**
     * The requested number of partitions. Should be incorporated by {@link FlinkExecutionOperator}s.
     */
    private int numDefaultPartitions;


    public FlinkExecutor(FlinkPlatform flinkPlatform, Job job) {
        super(job);
        this.platform = flinkPlatform;
        this.flinkContextReference = this.platform.getFlinkContext(job);
        this.fee = this.flinkContextReference.get();
        this.numDefaultPartitions = (int)this.getConfiguration().getLongProperty("rheem.flink.paralelism");
        this.fee.setParallelism(this.numDefaultPartitions);
        this.flinkContextReference.noteObtainedReference();
    }

    @Override
    protected Tuple<List<ChannelInstance>, PartialExecution> execute(
                                            ExecutionTask task,
                                            List<ChannelInstance> inputChannelInstances,
                                            OptimizationContext.OperatorContext producerOperatorContext,
                                            boolean isRequestEagerExecution) {
        // Provide the ChannelInstances for the output of the task.
        final ChannelInstance[] outputChannelInstances = task.getOperator().createOutputChannelInstances(
                this, task, producerOperatorContext, inputChannelInstances
        );

        // Execute.
        final Collection<ExecutionLineageNode> executionLineageNodes;
        final Collection<ChannelInstance> producedChannelInstances;
        // TODO: Use proper progress estimator.
        this.job.reportProgress(task.getOperator().getName(), 50);

        long startTime = System.currentTimeMillis();
        try {
            final Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> results =
                    cast(task.getOperator()).evaluate(
                            toArray(inputChannelInstances),
                            outputChannelInstances,
                            this,
                            producerOperatorContext
                    );
            executionLineageNodes = results.getField0();
            producedChannelInstances = results.getField1();
        } catch (Exception e) {
            throw new RheemException(String.format("Executing %s failed.", task), e);
        }
        long endTime = System.currentTimeMillis();
        long executionDuration = endTime - startTime;
        this.job.reportProgress(task.getOperator().getName(), 100);

        // Check how much we executed.
        PartialExecution partialExecution = this.createPartialExecution(executionLineageNodes, executionDuration);

        if (partialExecution == null && executionDuration > 10) {
            this.logger.warn("Execution of {} took suspiciously long ({}).", task, Formats.formatDuration(executionDuration));
        }

        // Collect any cardinality updates.
        this.registerMeasuredCardinalities(producedChannelInstances);

        // Warn if requested eager execution did not take place.
        if (isRequestEagerExecution ){
            if( partialExecution == null) {
                this.logger.info("{} was not executed eagerly as requested.", task);
            }else {
                try {
                    //TODO validate the execute in different contexts
                    //this.fee.execute();
                } catch (Exception e) {
                    throw new RheemException(e);
                }
            }
        }
        return new Tuple<>(Arrays.asList(outputChannelInstances), partialExecution);
    }

    @Override
    public void dispose() {
        super.dispose();
        this.flinkContextReference.noteDiscardedReference(false);
    }

    @Override
    public Platform getPlatform() {
        return this.platform;
    }

    private static FlinkExecutionOperator cast(ExecutionOperator executionOperator) {
        return (FlinkExecutionOperator) executionOperator;
    }

    private static ChannelInstance[] toArray(List<ChannelInstance> channelInstances) {
        final ChannelInstance[] array = new ChannelInstance[channelInstances.size()];
        return channelInstances.toArray(array);
    }

    /**
     * Provide a {@link FunctionCompiler}.
     *
     * @return the {@link FunctionCompiler}
     */
    public FunctionCompiler getCompiler() {
        return this.compiler;
    }

    public int getNumDefaultPartitions(){
        return this.numDefaultPartitions;
    }
}
