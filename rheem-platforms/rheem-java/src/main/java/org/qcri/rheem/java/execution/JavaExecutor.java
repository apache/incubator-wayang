package org.qcri.rheem.java.execution;

import org.qcri.rheem.core.function.ExtendedFunction;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.PushExecutorTemplate;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.channels.JavaChannelManager;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.util.Arrays;
import java.util.List;

/**
 * {@link Executor} implementation for the {@link JavaPlatform}.
 */
public class JavaExecutor extends PushExecutorTemplate<ChannelExecutor> {

    private final JavaPlatform platform;

    public FunctionCompiler compiler = new FunctionCompiler();

    public JavaExecutor(JavaPlatform javaPlatform) {
        this.platform = javaPlatform;
    }

    @Override
    public JavaPlatform getPlatform() {
        return this.platform;
    }

    @Override
    protected void open(ExecutionTask task, List<ChannelExecutor> inputChannelInstances) {
        cast(task.getOperator()).open(toArray(inputChannelInstances), this.compiler);
    }

    @Override
    protected List<ChannelExecutor> execute(ExecutionTask task, List<ChannelExecutor> inputChannelInstances, boolean isForceExecution) {
        // Provide the ChannelExecutors for the output of the task.
        final ChannelExecutor[] outputChannelExecutors = this.createOutputChannelExecutors(task);

        // Execute.
        cast(task.getOperator()).evaluate(toArray(inputChannelInstances), outputChannelExecutors, this.compiler);

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
        final JavaChannelManager channelManager = this.getPlatform().getChannelManager();
        ChannelExecutor[] channelExecutors = new ChannelExecutor[task.getNumOuputChannels()];
        for (int outputIndex = 0; outputIndex < channelExecutors.length; outputIndex++) {
            final Channel outputChannel = task.getOutputChannel(outputIndex);
            channelExecutors[outputIndex] = channelManager.createChannelExecutor(outputChannel);
        }
        return channelExecutors;
    }

    private static JavaExecutionOperator cast(ExecutionOperator executionOperator) {
        return (JavaExecutionOperator) executionOperator;
    }

    private static ChannelExecutor[] toArray(List<ChannelExecutor> channelExecutors) {
        final ChannelExecutor[] array = new ChannelExecutor[channelExecutors.size()];
        return channelExecutors.toArray(array);
    }

    public static void openFunction(JavaExecutionOperator operator, Object function, ChannelExecutor[] inputs) {
        if (function instanceof ExtendedFunction) {
            ExtendedFunction extendedFunction = (ExtendedFunction) function;
            extendedFunction.open(new JavaExecutionContext(operator, inputs));
        }
    }
}
