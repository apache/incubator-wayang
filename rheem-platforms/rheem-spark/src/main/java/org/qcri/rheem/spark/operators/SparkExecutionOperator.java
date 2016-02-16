package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDDLike;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.List;

/**
 * Execution operator for the Java platform.
 */
public interface SparkExecutionOperator extends ExecutionOperator {

    @Override
    default SparkPlatform getPlatform() {
        return SparkPlatform.getInstance();
    }

    /**
     * Evaluates this operator. Takes a set of Java {@link JavaRDDLike}s according to the operator inputs and produces
     * a set of {@link JavaRDDLike}s according to the operator outputs -- unless the operator is a sink, then it triggers
     * execution.
     *
     * @param inputRdds {@link JavaRDDLike}s that satisfy the inputs of this operator
     * @param compiler  compiles functions used by the operator
     * @return {@link JavaRDDLike}s that statisfy the outputs of this operator
     */
    JavaRDDLike[] evaluate(JavaRDDLike[] inputRdds, FunctionCompiler compiler, SparkExecutor sparkExecutor);

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
}
