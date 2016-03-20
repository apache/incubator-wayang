package org.qcri.rheem.spark.platform;

import org.apache.spark.api.java.JavaSparkContext;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.ExtendedFunction;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ExecutionProfile;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.platform.PushExecutorTemplate;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.channels.SparkChannelManager;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.execution.SparkExecutionContext;
import org.qcri.rheem.spark.operators.SparkExecutionOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * {@link Executor} implementation for the {@link SparkPlatform}.
 */
public class SparkExecutor extends PushExecutorTemplate<ChannelExecutor> {

    public final JavaSparkContext sc;

    public FunctionCompiler compiler = new FunctionCompiler();

    private final SparkPlatform platform;

    public SparkExecutor(SparkPlatform platform) {
        this.platform = platform;
        this.sc = this.platform.getSparkContext();
    }
    @Override
    protected void open(ExecutionTask task, List<ChannelExecutor> inputChannelInstances) {
        // Nothing to do. Opening is handled in #execute(...).
    }

    @Override
    protected List<ChannelExecutor> execute(ExecutionTask task, List<ChannelExecutor> inputChannelInstances, boolean isForceExecution) {
        // Provide the ChannelExecutors for the output of the task.
        final ChannelExecutor[] outputChannelExecutors = this.createOutputChannelExecutors(task);

        // Execute.
        cast(task.getOperator()).evaluate(toArray(inputChannelInstances), outputChannelExecutors, this.compiler, this);

        // Force execution if necessary.
        if (isForceExecution) {
            for (ChannelExecutor outputChannelExecutor : outputChannelExecutors) {
                if (outputChannelExecutor == null || !outputChannelExecutor.ensureExecution()) {
                    this.logger.warn("Execution of {} might not have been enforced properly. " +
                                    "This might break the execution or cause side-effects with the re-optimization.",
                            task);
                }
            }
        }

        return Arrays.asList(outputChannelExecutors);
    }


    private ChannelExecutor[] createOutputChannelExecutors(ExecutionTask task) {
        final SparkChannelManager channelManager = this.getPlatform().getChannelManager();
        ChannelExecutor[] channelExecutors = new ChannelExecutor[task.getNumOuputChannels()];
        for (int outputIndex = 0; outputIndex < channelExecutors.length; outputIndex++) {
            final Channel outputChannel = task.getOutputChannel(outputIndex);
            channelExecutors[outputIndex] = channelManager.createChannelExecutor(outputChannel, this);
        }
        return channelExecutors;
    }

    private static SparkExecutionOperator cast(ExecutionOperator executionOperator) {
        return (SparkExecutionOperator) executionOperator;
    }

    private static ChannelExecutor[] toArray(List<ChannelExecutor> channelExecutors) {
        final ChannelExecutor[] array = new ChannelExecutor[channelExecutors.size()];
        return channelExecutors.toArray(array);
    }

    public static void openFunction(SparkExecutionOperator operator, Object function, ChannelExecutor[] inputs) {
        if (function instanceof ExtendedFunction) {
            ExtendedFunction extendedFunction = (ExtendedFunction) function;
            extendedFunction.open(new SparkExecutionContext(operator, inputs));
        }
    }

    @Override
    public SparkPlatform getPlatform() {
        return this.platform;
    }
}
