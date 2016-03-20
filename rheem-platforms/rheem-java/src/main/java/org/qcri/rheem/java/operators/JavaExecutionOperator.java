package org.qcri.rheem.java.operators;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.List;
import java.util.stream.Stream;

/**
 * Execution operator for the Java platform.
 */
public interface JavaExecutionOperator extends ExecutionOperator {

    @Override
    default JavaPlatform getPlatform() {
        return JavaPlatform.getInstance();
    }

    /**
     * When this instance is not yet initialized, this method is called.
     *
     * @param inputs   {@link ChannelExecutor}s that satisfy the inputs of this operator
     * @param compiler compiles functions used by this instance
     */
    default void open(ChannelExecutor[] inputs, FunctionCompiler compiler) {
        // Do nothing by default.
    }

    /**
     * Evaluates this operator. Takes a set of Java {@link Stream}s according to the operator inputs and produces
     * a set of {@link Stream}s according to the operator outputs -- unless the operator is a sink, then it triggers
     * execution.
     *
     * @param inputs   {@link ChannelExecutor}s that satisfy the inputs of this operator
     * @param inputs   {@link ChannelExecutor}s that collect the outputs of this operator
     * @param compiler compiles functions used by the operator
     */
    void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler);

    @Override
    default List<ChannelDescriptor> getSupportedInputChannels(int index) {
        if (this.getInput(index).isBroadcast()) {
            return this.getPlatform().getChannelManager().getSupportedBroadcastChannels();
        } else {
            return this.getPlatform().getChannelManager().getSupportedChannels();
        }
    }

    @Override
    default List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return this.getPlatform().getChannelManager().getSupportedChannels();
    }

    default void instrumentSink(ChannelExecutor channelExecutor) {
        throw new RheemException(String.format("Cannot instrument %s.", this));
    }
}
