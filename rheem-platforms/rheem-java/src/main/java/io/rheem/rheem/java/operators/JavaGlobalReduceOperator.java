package io.rheem.rheem.java.operators;

import io.rheem.rheem.basic.operators.GlobalReduceOperator;
import io.rheem.rheem.core.api.Configuration;
import io.rheem.rheem.core.function.ReduceDescriptor;
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
import java.util.Optional;
import java.util.function.BinaryOperator;

/**
 * Java implementation of the {@link GlobalReduceOperator}.
 */
public class JavaGlobalReduceOperator<Type>
        extends GlobalReduceOperator<Type>
        implements JavaExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type             type of the reduce elements (i.e., type of {@link #getInput()} and {@link #getOutput()})
     * @param reduceDescriptor describes the reduction to be performed on the elements
     */
    public JavaGlobalReduceOperator(DataSetType<Type> type,
                                    ReduceDescriptor<Type> reduceDescriptor) {
        super(reduceDescriptor, type);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaGlobalReduceOperator(GlobalReduceOperator<Type> that) {
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

        final BinaryOperator<Type> reduceFunction = javaExecutor.getCompiler().compile(this.reduceDescriptor);
        JavaExecutor.openFunction(this, reduceFunction, inputs, operatorContext);

        final Optional<Type> reduction = ((JavaChannelInstance) inputs[0]).<Type>provideStream().reduce(reduceFunction);
        ((CollectionChannel.Instance) outputs[0]).accept(reduction.isPresent() ?
                Collections.singleton(reduction.get()) :
                Collections.emptyList());

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.java.globalreduce.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                JavaExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.reduceDescriptor, configuration);
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
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }

}
