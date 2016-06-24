package org.qcri.rheem.spark.platform;

import org.apache.spark.api.java.JavaSparkContext;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.ExtendedFunction;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.PartialExecution;
import org.qcri.rheem.core.platform.PushExecutorTemplate;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.execution.SparkExecutionContext;
import org.qcri.rheem.spark.operators.SparkExecutionOperator;

import java.util.Arrays;
import java.util.List;

/**
 * {@link Executor} implementation for the {@link SparkPlatform}.
 */
public class SparkExecutor extends PushExecutorTemplate {

    /**
     * Reference to a {@link JavaSparkContext} to be used by this instance.
     */
    private final SparkContextReference sparkContextReference;

    /**
     * The {@link JavaSparkContext} to be used by this instance.
     *
     * @see #sparkContextReference
     */
    public final JavaSparkContext sc;

    public FunctionCompiler compiler = new FunctionCompiler();

    private final SparkPlatform platform;

    public SparkExecutor(SparkPlatform platform, Job job) {
        super(job);
        this.platform = platform;
        this.sparkContextReference = this.platform.getSparkContext(job);
        this.sparkContextReference.noteObtainedReference();
        this.sc = this.sparkContextReference.get();
    }

    @Override
    protected void open(ExecutionTask task, List<ChannelInstance> inputChannelInstances) {
        // Nothing to do. Opening is handled in #execute(...).
    }

    @Override
    protected Tuple<List<ChannelInstance>, PartialExecution> execute(ExecutionTask task,
                                                                     List<ChannelInstance> inputChannelInstances,
                                                                     OptimizationContext.OperatorContext producerOperatorContext,
                                                                     boolean isForceExecution) {
        // Provide the ChannelInstances for the output of the task.
        final ChannelInstance[] outputChannelInstances = this.createOutputChannelInstances(
                task, producerOperatorContext, inputChannelInstances
        );

        // Execute.
        long startTime = System.currentTimeMillis();
        try {
            cast(task.getOperator()).evaluate(toArray(inputChannelInstances), outputChannelInstances, this.compiler, this);
        } catch (Exception e) {
            throw new RheemException(String.format("Executing %s failed.", task), e);
        }
        long endTime = System.currentTimeMillis();
        long executionDuration = endTime - startTime;

        // Check how much we executed.
        PartialExecution partialExecution = this.handleLazyChannelLineage(
                task, inputChannelInstances, producerOperatorContext, outputChannelInstances, executionDuration
        );

        // Force execution if necessary.
        if (isForceExecution) {
            for (ChannelInstance outputChannelInstance : outputChannelInstances) {
                if (outputChannelInstance == null || !outputChannelInstance.getChannel().isReusable()) {
                    this.logger.warn("Execution of {} might not have been enforced properly. " +
                                    "This might break the execution or cause side-effects with the re-optimization.",
                            task);
                }
            }
        }

        return new Tuple<>(Arrays.asList(outputChannelInstances), partialExecution);
    }

    private static SparkExecutionOperator cast(ExecutionOperator executionOperator) {
        return (SparkExecutionOperator) executionOperator;
    }

    private static ChannelInstance[] toArray(List<ChannelInstance> channelInstances) {
        final ChannelInstance[] array = new ChannelInstance[channelInstances.size()];
        return channelInstances.toArray(array);
    }

    public static void openFunction(SparkExecutionOperator operator, Object function, ChannelInstance[] inputs) {
        if (function instanceof ExtendedFunction) {
            ExtendedFunction extendedFunction = (ExtendedFunction) function;
            extendedFunction.open(new SparkExecutionContext(operator, inputs));
        }
    }

    @Override
    public SparkPlatform getPlatform() {
        return this.platform;
    }

    public Configuration getConfiguration() {
        return this.job.getConfiguration();
    }

    @Override
    public void dispose() {
        super.dispose();
        this.sparkContextReference.noteDiscardedReference(true);
    }
}
