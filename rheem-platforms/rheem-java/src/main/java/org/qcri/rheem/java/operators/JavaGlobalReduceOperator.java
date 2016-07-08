package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.GlobalReduceOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.ReduceDescriptor;
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
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
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

    @Override
    public void open(ChannelInstance[] inputs, FunctionCompiler compiler) {
        final BiFunction<Type, Type, Type> udf = compiler.compile(this.reduceDescriptor);
        JavaExecutor.openFunction(this, udf, inputs);
    }

    @Override
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final BinaryOperator<Type> reduceFunction = compiler.compile(this.reduceDescriptor);
        JavaExecutor.openFunction(this, reduceFunction, inputs);

        final Optional<Type> reduction = ((JavaChannelInstance) inputs[0]).<Type>provideStream().reduce(reduceFunction);
        ((CollectionChannel.Instance) outputs[0]).accept(reduction.isPresent() ?
                Collections.singleton(reduction.get()) :
                Collections.emptyList());
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final NestableLoadProfileEstimator estimator = NestableLoadProfileEstimator.parseSpecification(
                configuration.getStringProperty("rheem.java.globalreduce.load")
        );
        final LoadProfileEstimator udfEstimator = configuration
                .getFunctionLoadProfileEstimatorProvider()
                .provideFor(this.reduceDescriptor);
        estimator.nest(udfEstimator);
        return Optional.of(estimator);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaGlobalReduceOperator<>(this.getInputType(), this.getReduceDescriptor());
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
