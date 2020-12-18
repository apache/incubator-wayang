package org.apache.incubator.wayang.java.operators;

import org.apache.incubator.wayang.basic.operators.LocalCallbackSink;
import org.apache.incubator.wayang.core.optimizer.OptimizationContext;
import org.apache.incubator.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.incubator.wayang.core.platform.ChannelDescriptor;
import org.apache.incubator.wayang.core.platform.ChannelInstance;
import org.apache.incubator.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.incubator.wayang.core.types.DataSetType;
import org.apache.incubator.wayang.core.util.Tuple;
import org.apache.incubator.wayang.java.channels.CollectionChannel;
import org.apache.incubator.wayang.java.channels.JavaChannelInstance;
import org.apache.incubator.wayang.java.channels.StreamChannel;
import org.apache.incubator.wayang.java.execution.JavaExecutor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/**
 * Implementation of the {@link LocalCallbackSink} operator for the Java platform.
 */
public class JavaLocalCallbackSink<T extends Serializable> extends LocalCallbackSink<T> implements JavaExecutionOperator {
    /**
     * Creates a new instance.
     *
     * @param callback callback that is executed locally for each incoming data unit
     * @param type     type of the incoming elements
     */
    public JavaLocalCallbackSink(Consumer<T> callback, DataSetType type) {
        super(callback, type);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaLocalCallbackSink(LocalCallbackSink<T> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor executor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        ((JavaChannelInstance) inputs[0]).<T>provideStream().forEach(this.callback);

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaLocalCallbackSink<>(this.callback, this.getType());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.java.localcallbacksink.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        throw new UnsupportedOperationException(String.format("%s does not have outputs.", this));
    }

}
