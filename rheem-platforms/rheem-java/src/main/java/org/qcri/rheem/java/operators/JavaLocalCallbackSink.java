package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
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
    public void evaluate(ChannelInstance[] inputs,
                         ChannelInstance[] outputs,
                         JavaExecutor executor,
                         OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        ((JavaChannelInstance) inputs[0]).<T>provideStream().forEach(this.callback);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaLocalCallbackSink<>(this.callback, this.getType());
    }


    @Override
    public Optional<LoadProfileEstimator<ExecutionOperator>> createLoadProfileEstimator(Configuration configuration) {
        final NestableLoadProfileEstimator<ExecutionOperator> estimator = LoadProfileEstimators.createFromJuelSpecification(
                configuration.getStringProperty("rheem.java.localcallbacksink.load")
        );
        return Optional.of(estimator);
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

    @Override
    public boolean isExecutedEagerly() {
        return true;
    }
}
