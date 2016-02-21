package org.qcri.rheem.spark.operators;

import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.List;

/**
 * Execution operator for the {@link SparkPlatform}.
 */
public interface SparkExecutionOperator extends ExecutionOperator {

    @Override
    default SparkPlatform getPlatform() {
        return SparkPlatform.getInstance();
    }

    /**
     * Evaluates this operator. Takes a set of {@link ChannelExecutor}s according to the operator inputs and manipulates
     * a set of {@link ChannelExecutor}s according to the operator outputs -- unless the operator is a sink, then it triggers
     * execution.
     *
     * @param inputs        {@link ChannelExecutor}s that satisfy the inputs of this operator
     * @param outputs       {@link ChannelExecutor}s that accept the outputs of this operator
     * @param compiler      compiles functions used by the operator
     * @param sparkExecutor {@link SparkExecutor} that executes this instance
     */
    void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor);

    @Override
    default List<Class<? extends Channel>> getSupportedInputChannels(int index) {
        final InputSlot<?> input = this.getInput(index);
        return input.isBroadcast() ?
                this.getPlatform().getChannelManager().getSupportedBroadcastChannels() :
                this.getPlatform().getChannelManager().getSupportedChannels();
    }

    @Override
    default List<Class<? extends Channel>> getSupportedOutputChannels(int index) {
        return this.getPlatform().getChannelManager().getAllSupportedChannels();
    }

//    default void instrument(boolean shouldInstrument) {
//        if (shouldInstrument) {
//            throw new RuntimeException(String.format("%s#instrument(true) not yet implemented!", this));
//        }
//    }
}
