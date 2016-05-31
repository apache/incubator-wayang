package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.CartesianOperator;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;


/**
 * Spark implementation of the {@link CartesianOperator}.
 */
public class SparkCartesianOperator<InputType0, InputType1>
        extends CartesianOperator<InputType0, InputType1>
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     */
    public SparkCartesianOperator(DataSetType<InputType0> inputType0, DataSetType<InputType1> inputType1) {
        super(inputType0, inputType1);
    }

    @Override
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final RddChannel.Instance input0 = (RddChannel.Instance) inputs[0];
        final RddChannel.Instance input1 = (RddChannel.Instance) inputs[1];
        final RddChannel.Instance output = (RddChannel.Instance) outputs[0];


        final JavaRDD<InputType0> rdd0 = input0.provideRdd();
        final JavaRDD<InputType1> rdd1 = input1.provideRdd();
        final JavaRDD<Tuple2<InputType0, InputType1>> crossProduct = rdd0
                .cartesian(rdd1)
                .map(scalaTuple -> new Tuple2<>(scalaTuple._1, scalaTuple._2));

        output.accept(crossProduct, sparkExecutor);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkCartesianOperator<>(this.getInputType0(), this.getInputType1());
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(org.qcri.rheem.core.api.Configuration configuration) {
        final NestableLoadProfileEstimator mainEstimator = new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(2, 1, .9d, (inputCards, outputCards) -> 20000000 * inputCards[0] + 10000000 * inputCards[1] + 100 * outputCards[0] + 5500000000L),
                new DefaultLoadEstimator(2, 1, .9d, (inputCards, outputCards) -> 0),
                new DefaultLoadEstimator(2, 1, .9d, (inputCards, outputCards) -> 0),
                new DefaultLoadEstimator(2, 1, .9d, (inputCards, outputCards) -> 20000 * (inputCards[0] + inputCards[1]) + 1700000),
                0.1d,
                1000
        );

        return Optional.of(mainEstimator);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }
}
