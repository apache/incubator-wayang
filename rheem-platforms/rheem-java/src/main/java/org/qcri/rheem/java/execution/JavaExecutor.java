package org.qcri.rheem.java.execution;

import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.function.ExtendedFunction;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.PushExecutorTemplate;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.util.Arrays;
import java.util.List;

/**
 * {@link Executor} implementation for the {@link JavaPlatform}.
 */
public class JavaExecutor extends PushExecutorTemplate<JavaChannelInstance> {

    private final JavaPlatform platform;

    public FunctionCompiler compiler = new FunctionCompiler();

    public JavaExecutor(JavaPlatform javaPlatform, Job job) {
        super(job);
        this.platform = javaPlatform;
    }

    @Override
    public JavaPlatform getPlatform() {
        return this.platform;
    }

    @Override
    protected void open(ExecutionTask task, List<JavaChannelInstance> inputChannelInstances) {
        cast(task.getOperator()).open(toArray(inputChannelInstances), this.compiler);
    }

    @Override
    protected List<JavaChannelInstance> execute(ExecutionTask task, List<JavaChannelInstance> inputChannelInstances, boolean isForceExecution) {
        // Provide the ChannelInstances for the output of the task.
        final JavaChannelInstance[] outputChannelInstances = this.createOutputChannelInstances(task);

        // Execute.
        cast(task.getOperator()).evaluate(toArray(inputChannelInstances), outputChannelInstances, this.compiler);

        // Force execution if necessary.
        if (isForceExecution) {
            for (JavaChannelInstance outputChannelInstance : outputChannelInstances) {
                if (outputChannelInstance == null || !outputChannelInstance.getChannel().isReusable()) {
                    this.logger.warn("Execution of {} might not have been enforced properly. " +
                            "This might break the execution or cause side-effects with the re-optimization.",
                            task);
                }
            }
        }

        return Arrays.asList(outputChannelInstances);
    }


    private JavaChannelInstance[] createOutputChannelInstances(ExecutionTask task) {
        JavaChannelInstance[] channelInstances = new JavaChannelInstance[task.getNumOuputChannels()];
        for (int outputIndex = 0; outputIndex < channelInstances.length; outputIndex++) {
            final Channel outputChannel = task.getOutputChannel(outputIndex);
            channelInstances[outputIndex] = (JavaChannelInstance) outputChannel.createInstance();
        }
        return channelInstances;
    }

    private static JavaExecutionOperator cast(ExecutionOperator executionOperator) {
        return (JavaExecutionOperator) executionOperator;
    }

    private static JavaChannelInstance[] toArray(List<JavaChannelInstance> channelInstances) {
        final JavaChannelInstance[] array = new JavaChannelInstance[channelInstances.size()];
        return channelInstances.toArray(array);
    }

    public static void openFunction(JavaExecutionOperator operator, Object function, JavaChannelInstance[] inputs) {
        if (function instanceof ExtendedFunction) {
            ExtendedFunction extendedFunction = (ExtendedFunction) function;
            extendedFunction.open(new JavaExecutionContext(operator, inputs));
        }
    }
}
