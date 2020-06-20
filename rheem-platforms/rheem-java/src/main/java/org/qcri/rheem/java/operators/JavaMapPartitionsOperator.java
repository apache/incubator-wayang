package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.MapPartitionsOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.MapPartitionsDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Iterators;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.StreamSupport;

/**
 * Java implementation of the {@link MapPartitionsOperator}.
 */
public class JavaMapPartitionsOperator<InputType, OutputType>
        extends MapPartitionsOperator<InputType, OutputType>
        implements JavaExecutionOperator {

    /**
     * Creates a new instance.
     */
    public JavaMapPartitionsOperator(DataSetType<InputType> inputType, DataSetType<OutputType> outputType,
                                     MapPartitionsDescriptor<InputType, OutputType> functionDescriptor) {
        super(functionDescriptor, inputType, outputType);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaMapPartitionsOperator(MapPartitionsOperator<InputType, OutputType> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Function<Iterable<InputType>, Iterable<OutputType>> function =
                javaExecutor.getCompiler().compile(this.functionDescriptor);
        JavaExecutor.openFunction(this, function, inputs, operatorContext);
        final Iterable<OutputType> outputDataQuanta =
                function.apply(Iterators.wrapWithIterable(((JavaChannelInstance) inputs[0]).<InputType>provideStream().iterator()));

        ((StreamChannel.Instance) outputs[0]).accept(StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        outputDataQuanta.iterator(),
                        Spliterator.ORDERED),
                false
        ));

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaMapPartitionsOperator<>(this.getInputType(), this.getOutputType(), this.getFunctionDescriptor());
    }


    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.java.mappartitions.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                JavaExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.functionDescriptor, configuration);
        return optEstimator;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        if (this.getInput(index).isBroadcast()) return Collections.singletonList(CollectionChannel.DESCRIPTOR);
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

}
