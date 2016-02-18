package org.qcri.rheem.spark.platform;

import org.apache.commons.lang3.Validate;
import org.apache.spark.api.java.JavaSparkContext;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.platform.ExecutionProfile;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.channels.SparkChannelManager;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.operators.SparkExecutionOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class SparkExecutor implements Executor {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public final JavaSparkContext sc;

    public FunctionCompiler compiler = new FunctionCompiler();

    private Map<Channel, ChannelExecutor> establishedChannelExecutors = new HashMap<>();

    private final SparkPlatform platform;

    public SparkExecutor(SparkPlatform platform) {
        this.platform = platform;
        this.sc = this.platform.getSparkContext();
    }

    @Override
    public ExecutionProfile execute(ExecutionStage stage) {
        final Collection<ExecutionTask> terminalTasks = stage.getTerminalTasks();
        terminalTasks.forEach(this::execute);

        this.logger.warn("ExecutionProfiles not yet implemented for {}.", this);
        return new ExecutionProfile();
    }

    private void execute(ExecutionTask executionTask) {
        ChannelExecutor[] inputs = this.obtainInputs(executionTask);
        final SparkExecutionOperator sparkExecutionOperator = (SparkExecutionOperator) executionTask.getOperator();
        ChannelExecutor[] outputs = this.createOutputExecutors(executionTask);
        sparkExecutionOperator.evaluate(inputs, outputs, this.compiler, this);
        this.registerChannelExecutor(outputs, executionTask);
    }

    private ChannelExecutor[] createOutputExecutors(ExecutionTask executionTask) {
        final SparkChannelManager channelManager = this.getPlatform().getChannelManager();
        ChannelExecutor[] channelExecutors = new ChannelExecutor[executionTask.getOutputChannels().length];
        for (int outputIndex = 0; outputIndex < channelExecutors.length; outputIndex++) {
            final Channel output = executionTask.getOutputChannel(outputIndex);
            channelExecutors[outputIndex] = channelManager.createChannelExecutor(output);
        }
        return channelExecutors;
    }

    private ChannelExecutor[] obtainInputs(ExecutionTask executionTask) {
        ChannelExecutor[] inputs = new ChannelExecutor[executionTask.getOperator().getNumInputs()];
        for (int inputIndex = 0; inputIndex < inputs.length; inputIndex++) {
            Channel input = executionTask.getInputChannel(inputIndex);
            inputs[inputIndex] = this.getOrEstablishChannelExecutor(input);
        }
        return inputs;
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

    private void registerChannelExecutor(ChannelExecutor[] outputs, ExecutionTask executionTask) {
        for (int outputIndex = 0; outputIndex < executionTask.getOperator().getNumOutputs(); outputIndex++) {
            Channel channel = executionTask.getOutputChannel(outputIndex);
            final ChannelExecutor channelExecutor = outputs[outputIndex];
            Validate.notNull(channelExecutor);
            this.establishedChannelExecutors.put(channel, channelExecutor);
        }
    }

    @Override
    public void dispose() {
        this.establishedChannelExecutors.values().forEach(ChannelExecutor::dispose);
    }

    @Override
    public SparkPlatform getPlatform() {
        return this.platform;
    }
}
