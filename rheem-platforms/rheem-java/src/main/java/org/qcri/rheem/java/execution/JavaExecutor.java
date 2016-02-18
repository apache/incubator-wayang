package org.qcri.rheem.java.execution;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.ExtendedFunction;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.platform.ExecutionProfile;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.channels.JavaChannelManager;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.util.*;

/**
 * Dummy executor for the Java platform.
 */
public class JavaExecutor implements Executor {

    private final JavaPlatform platform;

    public FunctionCompiler compiler = new FunctionCompiler();

    private Map<Channel, ChannelExecutor> establishedChannelExecutors = new HashMap<>();

    private Set<Channel> instrumentedChannels = new HashSet<>();

    public JavaExecutor(JavaPlatform javaPlatform) {
        this.platform = javaPlatform;
    }

    @Override
    public JavaPlatform getPlatform() {
        return this.platform;
    }

    @Override
    public ExecutionProfile execute(ExecutionStage stage) {
        final Collection<ExecutionTask> terminalTasks = stage.getTerminalTasks();
        terminalTasks.forEach(this::execute);
        return this.assembleExecutionProfile();
    }

    private void execute(ExecutionTask executionTask) {
        // Instrument all stage-outbound channels.
        for (Channel channel : executionTask.getOutputChannels()) {
            if (channel.getConsumers().stream().anyMatch(consumer -> consumer.getStage() != executionTask.getStage())) {
                channel.markForInstrumentation();
            }
            if (channel.isMarkedForInstrumentation()) {
                this.instrumentedChannels.add(channel);
            }
        }

        ChannelExecutor[] inputChannels = this.obtainInputChannels(executionTask);
        final JavaExecutionOperator javaExecutionOperator = (JavaExecutionOperator) executionTask.getOperator();
        ChannelExecutor[] outputChannels = this.createOutputChannelExecutors(executionTask);
        javaExecutionOperator.evaluate(inputChannels, outputChannels, this.compiler);
        this.registerChannelExecutor(outputChannels, executionTask);
    }

    private ChannelExecutor[] createOutputChannelExecutors(ExecutionTask executionTask) {
        final JavaChannelManager channelManager = this.getPlatform().getChannelManager();
        ChannelExecutor[] channelExecutors = new ChannelExecutor[executionTask.getOutputChannels().length];
        for (int outputIndex = 0; outputIndex < channelExecutors.length; outputIndex++) {
            final Channel outputChannel = executionTask.getOutputChannel(outputIndex);
            channelExecutors[outputIndex] = channelManager.createChannelExecutor(outputChannel);
        }
        return channelExecutors;
    }

    private ChannelExecutor[] obtainInputChannels(ExecutionTask executionTask) {
        ChannelExecutor[] inputChannels = new ChannelExecutor[executionTask.getOperator().getNumInputs()];
        for (int inputIndex = 0; inputIndex < inputChannels.length; inputIndex++) {
            Channel inputChannel = executionTask.getInputChannel(inputIndex);
            inputChannels[inputIndex] = this.getOrEstablishChannelExecutor(inputChannel);
        }
        return inputChannels;
    }

    private ChannelExecutor getOrEstablishChannelExecutor(Channel channel) {
        for (int numTry = 0; numTry < 2; numTry++) {
            final ChannelExecutor channelExecutor = this.establishedChannelExecutors.get(channel);
            if (channelExecutor != null) {
                return channelExecutor;
            }

            this.execute(channel.getProducer());
        }

        throw new RheemException("Execution failed: could not obtain data for " + channel);
    }

    private void registerChannelExecutor(ChannelExecutor[] outputChannels, ExecutionTask executionTask) {
        for (int outputIndex = 0; outputIndex < executionTask.getOutputChannels().length; outputIndex++) {
            Channel channel = executionTask.getOutputChannels()[outputIndex];
            final ChannelExecutor channelExecutor = outputChannels[outputIndex];
            Validate.notNull(channelExecutor);
            this.establishedChannelExecutors.put(channel, channelExecutor);
        }
    }

    private ExecutionProfile assembleExecutionProfile() {
        ExecutionProfile executionProfile = new ExecutionProfile();
        final Map<Channel, Long> cardinalities = executionProfile.getCardinalities();
        for (Channel channel : this.instrumentedChannels) {
            final ChannelExecutor channelExecutor = this.establishedChannelExecutors.get(channel);
            assert channelExecutor != null : String.format("Could not find a Channel executor for %s.", channel);
            cardinalities.put(channel, channelExecutor.getCardinality());
        }
        return executionProfile;
    }

    public static void openFunction(JavaExecutionOperator operator, Object function, ChannelExecutor[] inputs) {
        if (function instanceof ExtendedFunction) {
            ExtendedFunction extendedFunction = (ExtendedFunction) function;
            extendedFunction.open(new JavaExecutionContext(operator, inputs));
        }
    }

    @Override
    public void dispose() {
        this.establishedChannelExecutors.clear();
    }
}
