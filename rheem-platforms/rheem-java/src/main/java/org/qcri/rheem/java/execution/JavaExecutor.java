package org.qcri.rheem.java.execution;

import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.ExtendedFunction;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.PushExecutorTemplate;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.util.Arrays;
import java.util.List;

/**
 * {@link Executor} implementation for the {@link JavaPlatform}.
 */
public class JavaExecutor extends PushExecutorTemplate {

    private final JavaPlatform platform;

    private final FunctionCompiler compiler;

    public JavaExecutor(JavaPlatform javaPlatform, Job job) {
        super(job);
        this.platform = javaPlatform;
        this.compiler = new FunctionCompiler(job.getConfiguration());
    }

    @Override
    public JavaPlatform getPlatform() {
        return this.platform;
    }

    @Override
    protected void open(ExecutionTask task, List<ChannelInstance> inputChannelInstances) {
        cast(task.getOperator()).open(toArray(inputChannelInstances), this.compiler);
    }

    @Override
    protected List<ChannelInstance> execute(ExecutionTask task, List<ChannelInstance> inputChannelInstances, boolean isForceExecution) {
        // Provide the ChannelInstances for the output of the task.
        final ChannelInstance[] outputChannelInstances = this.createOutputChannelInstances(task);

        // Execute.
        try {
            cast(task.getOperator()).evaluate(toArray(inputChannelInstances), outputChannelInstances, this.compiler);
        } catch (Exception e) {
            throw new RheemException(String.format("Executing %s failed.", task), e);
        }

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
            channelInstances[outputIndex] = outputChannel.createInstance(this);
        }
        return channelInstances;
    }

    private static JavaExecutionOperator cast(ExecutionOperator executionOperator) {
        return (JavaExecutionOperator) executionOperator;
    }

    private static ChannelInstance[] toArray(List<ChannelInstance> channelInstances) {
        final ChannelInstance[] array = new ChannelInstance[channelInstances.size()];
        return channelInstances.toArray(array);
    }

    public static void openFunction(JavaExecutionOperator operator, Object function, ChannelInstance[] inputs) {
        if (function instanceof ExtendedFunction) {
            ExtendedFunction extendedFunction = (ExtendedFunction) function;
            extendedFunction.open(new JavaExecutionContext(operator, inputs));
        }
    }
}
