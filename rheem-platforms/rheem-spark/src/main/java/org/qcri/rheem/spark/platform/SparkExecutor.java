package org.qcri.rheem.spark.platform;

import org.apache.spark.api.java.JavaSparkContext;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.function.ExtendedFunction;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.PushExecutorTemplate;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.execution.SparkExecutionContext;
import org.qcri.rheem.spark.operators.SparkExecutionOperator;

import java.util.Arrays;
import java.util.List;

/**
 * {@link Executor} implementation for the {@link SparkPlatform}.
 */
public class SparkExecutor extends PushExecutorTemplate {

    public final JavaSparkContext sc;

    public FunctionCompiler compiler = new FunctionCompiler();

    private final SparkPlatform platform;

    public SparkExecutor(SparkPlatform platform, Job job) {
        super(job);
        this.platform = platform;
        this.sc = this.platform.getSparkContext(job);
    }

    @Override
    protected void open(ExecutionTask task, List<ChannelInstance> inputChannelInstances) {
        // Nothing to do. Opening is handled in #execute(...).
    }

    @Override
    protected List<ChannelInstance> execute(ExecutionTask task, List<ChannelInstance> inputChannelInstances, boolean isForceExecution) {
        // Provide the ChannelInstances for the output of the task.
        final ChannelInstance[] outputChannelInstances = this.createOutputChannelInstances(task);

        // Execute.
        cast(task.getOperator()).evaluate(toArray(inputChannelInstances), outputChannelInstances, this.compiler, this);

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

        return Arrays.asList(outputChannelInstances);
    }

    private ChannelInstance[] createOutputChannelInstances(ExecutionTask task) {
        ChannelInstance[] channelInstances = new ChannelInstance[task.getNumOuputChannels()];
        for (int outputIndex = 0; outputIndex < channelInstances.length; outputIndex++) {
            final Channel outputChannel = task.getOutputChannel(outputIndex);
            channelInstances[outputIndex] = outputChannel.createInstance();
        }
        return channelInstances;
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

        this.getPlatform().closeSparkContext(this.sc);
    }
}
