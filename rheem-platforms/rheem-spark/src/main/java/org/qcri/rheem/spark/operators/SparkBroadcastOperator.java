package org.qcri.rheem.spark.operators;

import org.apache.spark.broadcast.Broadcast;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.spark.channels.BroadcastChannel;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Takes care of creating a {@link Broadcast} that can be used later on.
 */
public class SparkBroadcastOperator<Type> extends OperatorBase implements SparkExecutionOperator {

    public SparkBroadcastOperator(DataSetType<Type> type, OperatorContainer operatorContainer) {
        super(1, 1, false, operatorContainer);
        this.inputSlots[0] = new InputSlot<>("input", this, type);
        this.outputSlots[0] = new OutputSlot<>("output", this, type);
    }

    @Override
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final CollectionChannel.Instance input = (CollectionChannel.Instance) inputs[0];
        final BroadcastChannel.Instance output = (BroadcastChannel.Instance) outputs[0];

        final Collection<?> collection = input.provideCollection();
        final Broadcast<?> broadcast = sparkExecutor.sc.broadcast(collection);
        output.accept(broadcast);
    }

    @SuppressWarnings("unchecked")
    public DataSetType<Type> getType() {
        return (DataSetType<Type>) this.getInput(0).getType();
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkBroadcastOperator<>(this.getType(), this.getContainer());
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(org.qcri.rheem.core.api.Configuration configuration) {
        final String specification = configuration.getStringProperty("rheem.spark.broadcast.load");
        final NestableLoadProfileEstimator mainEstimator = NestableLoadProfileEstimator.parseSpecification(specification);
        return Optional.of(mainEstimator);
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
