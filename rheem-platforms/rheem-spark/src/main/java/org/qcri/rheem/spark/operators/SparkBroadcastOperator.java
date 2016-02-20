package org.qcri.rheem.spark.operators;

import org.apache.spark.broadcast.Broadcast;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.OperatorBase;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

import java.util.List;

/**
 * Takes care of creating a {@link Broadcast} that can be used later on.
 */
public class SparkBroadcastOperator<Type> extends OperatorBase implements SparkExecutionOperator {

    public SparkBroadcastOperator(DataSetType<Type> type) {
        super(1, 1, false, null);
        this.inputSlots[0] = new InputSlot<>("input", this, type);
        this.outputSlots[0] = new OutputSlot<>("output", this, type);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final List<?> collect = inputs[0].provideRdd().collect();
        final Broadcast<?> broadcast = sparkExecutor.sc.broadcast(collect);
        outputs[0].acceptBroadcast(broadcast);
    }

    @SuppressWarnings("unchecked")
    public DataSetType<Type> getType() {
        return (DataSetType<Type>) this.getInput(0).getType();
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkBroadcastOperator<>(this.getType());
    }
}
