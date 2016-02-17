package org.qcri.rheem.spark.operators;

import org.apache.spark.broadcast.Broadcast;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.OperatorBase;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

import java.util.List;

/**
 * Takes care of creating a {@link Broadcast} that can be used later on.
 */
public class SparkBroadcastOperator extends OperatorBase implements SparkExecutionOperator {

    public SparkBroadcastOperator() {
        super(1, 1, false, null);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final List<?> collect = inputs[0].provideRdd().collect();
        final Broadcast<?> broadcast = sparkExecutor.sc.broadcast(collect);
        outputs[0].acceptBroadcast(broadcast);
    }

    @Override
    public ExecutionOperator copy() {
        return new SparkBroadcastOperator();
    }
}
