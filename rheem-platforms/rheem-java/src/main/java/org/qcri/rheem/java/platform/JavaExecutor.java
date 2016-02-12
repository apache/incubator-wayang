package org.qcri.rheem.java.platform;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.channels.Channels;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Dummy executor for the Java platform.
 */
public class JavaExecutor implements Executor {

    public static final Executor.Factory FACTORY = JavaExecutor::new;

    public FunctionCompiler compiler = new FunctionCompiler();

    private Map<Channel, ChannelExecutor> establishedChannelExecutors = new HashMap<>();

    @Override
    public void execute(ExecutionStage stage) {
        final Collection<ExecutionTask> terminalTasks = stage.getTerminalTasks();
        terminalTasks.forEach(this::execute);
    }

    private void execute(ExecutionTask executionTask) {
        Stream[] inputStreams = obtainInputStreams(executionTask);
        final JavaExecutionOperator javaExecutionOperator = (JavaExecutionOperator) executionTask.getOperator();
        Stream[] outputStreams = javaExecutionOperator.evaluate(inputStreams, this.compiler);
        registerOutputStreams(outputStreams, executionTask);
    }

    private Stream[] obtainInputStreams(ExecutionTask executionTask) {
        Stream[] inputStreams = new Stream[executionTask.getInputChannels().length];
        for (int inputIndex = 0; inputIndex < inputStreams.length; inputIndex++) {
            Channel inputChannel = executionTask.getInputChannel(inputIndex);
            inputStreams[inputIndex] = this.getInputStreamFor(inputChannel);
        }
        return inputStreams;
    }

    private Stream<?> getInputStreamFor(Channel channel) {
        for (int numTry = 0; numTry < 2; numTry++) {
            final ChannelExecutor channelExecutor = this.establishedChannelExecutors.get(channel);
            if (channelExecutor != null) {
                return channelExecutor.provideStream();
            }

            this.execute(channel.getProducer());
        }

        throw new RheemException("Execution failed: could not obtain data for " + channel);
    }

    private void registerOutputStreams(Stream[] outputStreams, ExecutionTask executionTask) {
        for (int outputIndex = 0; outputIndex < executionTask.getOutputChannels().length; outputIndex++) {
            Channel channel = executionTask.getOutputChannels()[outputIndex];
            final ChannelExecutor channelExecutor = Channels.createChannelExecutor(channel);
            Validate.notNull(channelExecutor);
            channelExecutor.acceptStream(outputStreams[outputIndex]);
            this.establishedChannelExecutors.put(channel, channelExecutor);
        }
    }

    @Override
    public void evaluate(ExecutionOperator executionOperator) {
        if (!executionOperator.isSink()) {
            throw new IllegalArgumentException("Cannot evaluate execution operator: it is not a sink");
        }

        if (!(executionOperator instanceof JavaExecutionOperator)) {
            throw new IllegalStateException(String.format("Cannot evaluate execution operator: " +
                    "Execution plan contains non-Java operator %s.", executionOperator));
        }

        this.evaluate0((JavaExecutionOperator) executionOperator);
    }

    private Stream[] evaluate0(JavaExecutionOperator operator) {
        // Resolve all the input streams for this operator.
        Stream[] inputStreams = new Stream[operator.getNumInputs()];
        for (int i = 0; i < inputStreams.length; i++) {
            final OutputSlot outputSlot = operator.getInput(i).getOccupant();
            if (outputSlot == null) {
                throw new IllegalStateException("Cannot evaluate execution operator: There is an unsatisfied input.");
            }

            final Operator inputOperator = outputSlot.getOwner();
            if (!(inputOperator instanceof JavaExecutionOperator)) {
                throw new IllegalStateException(String.format("Cannot evaluate execution operator: " +
                        "Execution plan contains non-Java operator %s.", inputOperator));
            }

            Stream[] outputStreams = this.evaluate0((JavaExecutionOperator) inputOperator);
            int outputSlotIndex = 0;
            for (; outputSlot != inputOperator.getOutput(outputSlotIndex); outputSlotIndex++) ;
            inputStreams[i] = outputStreams[outputSlotIndex];
        }

        return operator.evaluate(inputStreams, this.compiler);
    }

    @Override
    public void dispose() {
        this.establishedChannelExecutors.clear();
    }
}
