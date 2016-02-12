package org.qcri.rheem.spark.platform;

import org.apache.commons.lang3.Validate;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.channels.Channels;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.operators.SparkExecutionOperator;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;


public class SparkExecutor implements Executor {

    public static final Executor.Factory FACTORY = SparkExecutor::new;

    public final JavaSparkContext sc = SparkPlatform.getInstance().getSparkContext();

    public FunctionCompiler compiler = new FunctionCompiler();

    private Map<Channel, ChannelExecutor> establishedChannelExecutors = new HashMap<>();

    @Override
    public void execute(ExecutionStage stage) {
        final Collection<ExecutionTask> terminalTasks = stage.getTerminalTasks();
        terminalTasks.forEach(this::execute);
    }

    @Override
    public void dispose() {
        establishedChannelExecutors.values().forEach(ChannelExecutor::dispose);
    }

    private void execute(ExecutionTask executionTask) {
        JavaRDDLike[] inputRdds = obtainInputRdds(executionTask);
        final SparkExecutionOperator sparkExecutionOperator = (SparkExecutionOperator) executionTask.getOperator();
        JavaRDDLike[] outputRdds = sparkExecutionOperator.evaluate(inputRdds, this.compiler, this);
        registerOutputRdds(outputRdds, executionTask);
    }

    private JavaRDDLike[] obtainInputRdds(ExecutionTask executionTask) {
        JavaRDDLike[] inputRdds = new JavaRDDLike[executionTask.getInputChannels().length];
        for (int inputIndex = 0; inputIndex < inputRdds.length; inputIndex++) {
            Channel inputChannel = executionTask.getInputChannel(inputIndex);
            inputRdds[inputIndex] = this.getInputRddFor(inputChannel);
        }
        return inputRdds;
    }

    private JavaRDDLike getInputRddFor(Channel channel) {
        for (int numTry = 0; numTry < 2; numTry++) {
            final ChannelExecutor channelExecutor = this.establishedChannelExecutors.get(channel);
            if (channelExecutor != null) {
                return channelExecutor.provideRdd();
            }

            this.execute(channel.getProducer());
        }

        throw new RheemException("Execution failed: could not obtain data for " + channel);
    }

    private void registerOutputRdds(JavaRDDLike[] outputStreams, ExecutionTask executionTask) {
        for (int outputIndex = 0; outputIndex < executionTask.getOutputChannels().length; outputIndex++) {
            Channel channel = executionTask.getOutputChannels()[outputIndex];
            final ChannelExecutor channelExecutor = Channels.createChannelExecutor(channel);
            Validate.notNull(channelExecutor);
            channelExecutor.acceptRdd(outputStreams[outputIndex]);
            this.establishedChannelExecutors.put(channel, channelExecutor);
        }
    }

    @Override
    public void evaluate(ExecutionOperator executionOperator) {
        if (!executionOperator.isSink()) {
            throw new IllegalArgumentException("Cannot evaluate execution operator: it is not a sink");
        }

        if (!(executionOperator instanceof SparkExecutionOperator)) {
            throw new IllegalStateException(String.format("Cannot evaluate execution operator: " +
                    "Execution plan contains non-Java operator %s.", executionOperator));
        }

        this.evaluate0((SparkExecutionOperator) executionOperator);
    }

    private JavaRDDLike[] evaluate0(SparkExecutionOperator operator) {
        // Resolve all the input streams for this operator.
        JavaRDDLike[] inputStreams = new JavaRDDLike[operator.getNumInputs()];
        for (int i = 0; i < inputStreams.length; i++) {
            final OutputSlot outputSlot = operator.getInput(i).getOccupant();
            if (outputSlot == null) {
                throw new IllegalStateException("Cannot evaluate execution operator: There is an unsatisfied input.");
            }

            final Operator inputOperator = outputSlot.getOwner();
            if (!(inputOperator instanceof SparkExecutionOperator)) {
                throw new IllegalStateException(String.format("Cannot evaluate execution operator: " +
                        "Execution plan contains non-Spark operator %s.", inputOperator));
            }

            JavaRDDLike[] outputStreams = this.evaluate0((SparkExecutionOperator) inputOperator);
            int outputSlotIndex = 0;
            for (; outputSlot != inputOperator.getOutput(outputSlotIndex); outputSlotIndex++) ;
            inputStreams[i] = outputStreams[outputSlotIndex];
        }

        return operator.evaluate(inputStreams, this.compiler, this);
    }
}
