package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.FlatMapOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
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

import java.util.*;
import java.util.function.Function;
import java.util.stream.StreamSupport;

/**
 * Java implementation of the {@link FlatMapOperator}.
 */
public class JavaFlatMapOperator<InputType, OutputType>
        extends FlatMapOperator<InputType, OutputType>
        implements JavaExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param functionDescriptor
     */
    public JavaFlatMapOperator(DataSetType inputType, DataSetType outputType,
                               FlatMapDescriptor<InputType, OutputType> functionDescriptor) {
        super(functionDescriptor, inputType, outputType);
    }

    @Override
    public void open(ChannelInstance[] inputs, FunctionCompiler compiler) {
        final Function<InputType, Iterable<OutputType>> udf = compiler.compile(this.functionDescriptor);
        JavaExecutor.openFunction(this, udf, inputs);
    }

    @Override
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Function<InputType, Iterable<OutputType>> flatmapFunction = compiler.compile(this.functionDescriptor);
        JavaExecutor.openFunction(this, flatmapFunction, inputs);

        ((StreamChannel.Instance) outputs[0]).accept(
                ((JavaChannelInstance) inputs[0]).<InputType>provideStream().flatMap(dataQuantum ->
                        StreamSupport.stream(
                                Spliterators.spliteratorUnknownSize(
                                        flatmapFunction.apply(dataQuantum).iterator(),
                                        Spliterator.ORDERED),
                                false
                        )
                )
        );
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaFlatMapOperator<>(this.getInputType(), this.getOutputType(), this.getFunctionDescriptor());
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(Configuration configuration) {
        final NestableLoadProfileEstimator estimator = NestableLoadProfileEstimator.parseSpecification(
                configuration.getStringProperty("rheem.java.flatmap.load")
        );
        estimator.nest(configuration.getFunctionLoadProfileEstimatorProvider().provideFor(this.getFunctionDescriptor()));
        return Optional.of(estimator);
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
