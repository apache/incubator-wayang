package org.qcri.rheem.spark.operators;

import org.apache.spark.broadcast.Broadcast;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.spark.channels.BroadcastChannel;
import org.qcri.rheem.spark.execution.SparkExecutor;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Takes care of creating a {@link Broadcast} that can be used later on.
 */
public class SparkBroadcastOperator<Type> extends UnaryToUnaryOperator<Type, Type> implements SparkExecutionOperator {

    public SparkBroadcastOperator(DataSetType<Type> type) {
        super(type, type, false);
    }

    public SparkBroadcastOperator(SparkBroadcastOperator<Type> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final CollectionChannel.Instance input = (CollectionChannel.Instance) inputs[0];
        final BroadcastChannel.Instance output = (BroadcastChannel.Instance) outputs[0];

        final Collection<?> collection = input.provideCollection();
        final Broadcast<?> broadcast = sparkExecutor.sc.broadcast(collection);
        output.accept(broadcast);

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    public boolean containsAction() {
        return true;
    }

    @SuppressWarnings("unchecked")
    public DataSetType<Type> getType() {
        return (DataSetType<Type>) this.getInput(0).getType();
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkBroadcastOperator<>(this);
    }


    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.spark.broadcast.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index == 0;
        return Collections.singletonList(BroadcastChannel.DESCRIPTOR);
    }

}
