package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.MaterializedGroupByOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadEstimator;
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

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Java implementation of the {@link MaterializedGroupByOperator}.
 */
public class JavaMaterializedGroupByOperator<Type, KeyType>
        extends MaterializedGroupByOperator<Type, KeyType>
        implements JavaExecutionOperator {


    public JavaMaterializedGroupByOperator(TransformationDescriptor<Type, KeyType> keyDescriptor,
                                           DataSetType<Type> inputType,
                                           DataSetType<Iterable<Type>> outputType) {
        super(keyDescriptor, inputType, outputType);
    }

    @Override
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Function<Type, KeyType> keyExtractor = compiler.compile(this.keyDescriptor);
        final Map<KeyType, List<Type>> collocation = ((JavaChannelInstance) inputs[0]).<Type>provideStream().collect(
                Collectors.groupingBy(
                        keyExtractor,
                        Collectors.toList())); // Not sure if this is thread-safe... Will we use #parallelStream()?

        ((CollectionChannel.Instance) outputs[0]).accept(collocation.values());
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(Configuration configuration) {
        return Optional.of(new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(1, 1, 0.9d, (inCards, outCards) -> 1200 * inCards[0] + 330000),
                LoadEstimator.createFallback(1, 1)
        ));
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaMaterializedGroupByOperator<>(this.getKeyDescriptor(), this.getInputType(), this.getOutputType());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }
}
