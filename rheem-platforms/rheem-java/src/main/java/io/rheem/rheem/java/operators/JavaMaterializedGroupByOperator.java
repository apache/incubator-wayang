package io.rheem.rheem.java.operators;

import io.rheem.rheem.basic.operators.MaterializedGroupByOperator;
import io.rheem.rheem.core.api.Configuration;
import io.rheem.rheem.core.function.TransformationDescriptor;
import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.optimizer.costs.LoadProfileEstimator;
import io.rheem.rheem.core.optimizer.costs.LoadProfileEstimators;
import io.rheem.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.rheem.core.platform.ChannelDescriptor;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.core.platform.lineage.ExecutionLineageNode;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.core.util.Tuple;
import io.rheem.rheem.java.channels.CollectionChannel;
import io.rheem.rheem.java.channels.JavaChannelInstance;
import io.rheem.rheem.java.channels.StreamChannel;
import io.rheem.rheem.java.execution.JavaExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaMaterializedGroupByOperator(MaterializedGroupByOperator<Type, KeyType> that) {
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

        final Function<Type, KeyType> keyExtractor = javaExecutor.getCompiler().compile(this.keyDescriptor);
        final Map<KeyType, List<Type>> collocation = ((JavaChannelInstance) inputs[0]).<Type>provideStream().collect(
                Collectors.groupingBy(
                        keyExtractor,
                        Collectors.toList())); // Not sure if this is thread-safe... Will we use #parallelStream()?

        ((CollectionChannel.Instance) outputs[0]).accept(collocation.values());

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.java.groupby.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                JavaExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.keyDescriptor, configuration);
        return optEstimator;
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
