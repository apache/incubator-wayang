package org.qcri.rheem.spark.execution;

import org.apache.spark.api.java.JavaSparkContext;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.PartialExecution;
import org.qcri.rheem.core.platform.PushExecutorTemplate;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.operators.SparkExecutionOperator;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.Arrays;
import java.util.Collection;
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

    /**
     * Compiler to create Spark UDFs.
     */
    public FunctionCompiler compiler = new FunctionCompiler();

    /**
     * Reference to the {@link SparkPlatform} that provides the {@link #sparkContextReference}.
     */
    private final SparkPlatform platform;

    /**
     * The requested number of partitions. Should be incorporated by {@link SparkExecutionOperator}s.
     */
    private final int numDefaultPartitions;

    /**
     * Counts the number of issued Spark actions.
     */
    private int numActions = 0;

    public SparkExecutor(SparkPlatform platform, Job job) {
        super(job);
        this.platform = platform;
        this.sparkContextReference = this.platform.getSparkContext(job);
        this.sparkContextReference.noteObtainedReference();
        this.sc = this.sparkContextReference.get();
        if (this.sc.getConf().contains("spark.executor.cores")) {
            this.numDefaultPartitions = 2 * this.sc.getConf().getInt("spark.executor.cores", -1);
        } else {
            this.numDefaultPartitions =
                    (int) (2 * this.getConfiguration().getLongProperty("rheem.spark.machines")
                            * this.getConfiguration().getLongProperty("rheem.spark.cores-per-machine"));
        }
    }

    @Override
    protected Tuple<List<ChannelInstance>, PartialExecution> execute(ExecutionTask task,
                                                                     List<ChannelInstance> inputChannelInstances,
                                                                     OptimizationContext.OperatorContext producerOperatorContext,
                                                                     boolean isForceExecution) {
        // Provide the ChannelInstances for the output of the task.
        final ChannelInstance[] outputChannelInstances = task.getOperator().createOutputChannelInstances(
                this, task, producerOperatorContext, inputChannelInstances
        );

        // Execute.
        final Collection<OptimizationContext.OperatorContext> operatorContexts;
        long startTime = System.currentTimeMillis();
        try {
            operatorContexts = cast(task.getOperator()).evaluate(
                    toArray(inputChannelInstances),
                    outputChannelInstances,
                    this,
                    producerOperatorContext
            );
        } catch (Exception e) {
            throw new RheemException(String.format("Executing %s failed.", task), e);
        }
        long endTime = System.currentTimeMillis();
        long executionDuration = endTime - startTime;

        // Check how much we executed.
        PartialExecution partialExecution = this.createPartialExecution(operatorContexts, executionDuration);
        if (partialExecution != null) {
            if (this.numActions == 0) partialExecution.addInitializedPlatform(SparkPlatform.getInstance());
            this.numActions++;
            this.job.addPartialExecutionMeasurement(partialExecution);
        }


        // Force execution if necessary.
        if (isForceExecution) {
            if (partialExecution == null) {
                this.logger.warn("Execution of {} might not have been enforced properly. " +
                                "This might break the execution or cause side-effects with the re-optimization.",
                        task);
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

    @Override
    public SparkPlatform getPlatform() {
        return this.platform;
    }

    /**
     * Hint to {@link SparkExecutionOperator}s on how many partitions they should request.
     *
     * @return the default number of partitions
     */
    public int getNumDefaultPartitions() {
        return this.numDefaultPartitions;
    }

    @Override
    public void dispose() {
        super.dispose();
        this.sparkContextReference.noteDiscardedReference(true);
    }

    /**
     * Provide a {@link FunctionCompiler}.
     *
     * @return the {@link FunctionCompiler}
     */
    public FunctionCompiler getCompiler() {
        return this.compiler;
    }
}
