package org.apache.incubator.wayang.java.operators;

import org.apache.incubator.wayang.basic.operators.MaterializedGroupByOperator;
import org.apache.incubator.wayang.core.api.Configuration;
import org.apache.incubator.wayang.core.function.TransformationDescriptor;
import org.apache.incubator.wayang.core.optimizer.OptimizationContext;
import org.apache.incubator.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.incubator.wayang.core.optimizer.costs.LoadProfileEstimators;
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
        return "wayang.java.groupby.load";
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
