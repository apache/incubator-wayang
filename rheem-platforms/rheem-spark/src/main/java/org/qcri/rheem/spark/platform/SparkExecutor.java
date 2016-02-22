package org.qcri.rheem.spark.platform;

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

import java.util.*;

/**
 * {@link Executor} implementation for the {@link SparkPlatform}.
 */
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
        // Instrument all stage-outbound channels.
        for (Channel channel : stage.getOutboundChannels()) {
            this.logger.debug("Marking {} for instrumentation.", channel);
            channel.markForInstrumentation(); // TODO: Instrumentation should be done in the CrossPlatformExecutor.
        }
        final Collection<ExecutionTask> terminalTasks = stage.getTerminalTasks();
        terminalTasks.forEach(this::forceExecution);

        return this.assembleExecutionProfile();
    }

    private void forceExecution(ExecutionTask terminalTask) {
        this.execute(terminalTask);
        for (Channel outputChannel : terminalTask.getOutputChannels()) {
            final ChannelExecutor channelExecutor = this.establishedChannelExecutors.get(outputChannel);
            assert channelExecutor != null : String.format("Could not find an executor for %s.", outputChannel);
            if (!channelExecutor.ensureExecution()) {
                this.logger.warn("Could not force execution of {}. This might break the execution or " +
                        "cause side-effects with the re-optimization.", outputChannel);
            }
        }
    }

    private void execute(ExecutionTask executionTask) {
        // We want to enforce a top-down creation order of the ChannelExecutors.
        ChannelExecutor[] outputs = this.createOutputExecutors(executionTask);
        ChannelExecutor[] inputs = this.obtainInputs(executionTask);
        final SparkExecutionOperator sparkExecutionOperator = (SparkExecutionOperator) executionTask.getOperator();
        this.logger.debug("Evaluating {}...", sparkExecutionOperator);
        sparkExecutionOperator.evaluate(inputs, outputs, this.compiler, this);
        this.registerChannelExecutor(outputs, executionTask);
    }

    private ChannelExecutor[] createOutputExecutors(ExecutionTask executionTask) {
        final SparkChannelManager channelManager = this.getPlatform().getChannelManager();
        ChannelExecutor[] channelExecutors = new ChannelExecutor[executionTask.getOutputChannels().length];
        for (int outputIndex = 0; outputIndex < channelExecutors.length; outputIndex++) {
            final Channel outputChannel = executionTask.getOutputChannel(outputIndex);
            final ChannelExecutor channelExecutor = channelManager.createChannelExecutor(outputChannel, this);
            channelExecutors[outputIndex] = channelExecutor;
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
        for (int outputIndex = 0; outputIndex < executionTask.getNumOuputChannels(); outputIndex++) {
            Channel channel = executionTask.getOutputChannel(outputIndex);
            final ChannelExecutor channelExecutor = outputs[outputIndex];
            assert channelExecutor != null;
            this.establishedChannelExecutors.put(channel, channelExecutor);
        }
    }

    private ExecutionProfile assembleExecutionProfile() {
        ExecutionProfile executionProfile = new ExecutionProfile();
        final Map<Channel, Long> cardinalities = executionProfile.getCardinalities();
        for (Map.Entry<Channel, ChannelExecutor> entry : this.establishedChannelExecutors.entrySet()) {
            final Channel channel = entry.getKey();
            if (!channel.isMarkedForInstrumentation()) continue;
            final ChannelExecutor channelExecutor = entry.getValue();
            final long cardinality = channelExecutor.getCardinality();
            if (cardinality == -1) {
                this.logger.warn("No cardinality available for {}, although it was requested.", channel);
            } else {
                cardinalities.put(channel, cardinality);
            }
        }
        return executionProfile;
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
