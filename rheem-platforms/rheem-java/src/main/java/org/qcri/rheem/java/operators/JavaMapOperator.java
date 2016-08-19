package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.TransformationDescriptor;
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
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Java implementation of the {@link org.qcri.rheem.basic.operators.MapOperator}.
 */
public class JavaMapOperator<InputType, OutputType>
        extends MapOperator<InputType, OutputType>
        implements JavaExecutionOperator {

    /**
     * Creates a new instance.
     */
    public JavaMapOperator(DataSetType inputType, DataSetType outputType, TransformationDescriptor<InputType, OutputType> functionDescriptor) {
        super(functionDescriptor, inputType, outputType);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaMapOperator(MapOperator<InputType, OutputType> that) {
        super(that);
    }

    @Override
    public void evaluate(ChannelInstance[] inputs,
                         ChannelInstance[] outputs,
                         JavaExecutor javaExecutor,
                         OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Function<InputType, OutputType> function = javaExecutor.getCompiler().compile(this.functionDescriptor);
        JavaExecutor.openFunction(this, function, inputs, operatorContext);
        ((StreamChannel.Instance) outputs[0]).accept(((JavaChannelInstance) inputs[0]).<InputType>provideStream().map(function));
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaMapOperator<>(this.getInputType(), this.getOutputType(), this.getFunctionDescriptor());
    }

    @Override
    public Optional<LoadProfileEstimator<ExecutionOperator>> createLoadProfileEstimator(Configuration configuration) {
        final NestableLoadProfileEstimator<ExecutionOperator> operatorEstimator = LoadProfileEstimators.createFromJuelSpecification(
                configuration.getStringProperty("rheem.java.map.load")
        );
        final LoadProfileEstimator<?> functionEstimator =
                configuration.getFunctionLoadProfileEstimatorProvider().provideFor(this.getFunctionDescriptor());
        operatorEstimator.nest(functionEstimator);
        return Optional.of(operatorEstimator);
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

    @Override
    public boolean isExecutedEagerly() {
        return false;
    }
}
