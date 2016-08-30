package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.basic.operators.SampleOperator;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.BroadcastChannel;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.execution.SparkExecutor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;


/**
 * Spark implementation of the {@link SparkBernoulliSampleOperator}.
 */
public class SparkBernoulliSampleOperator<Type>
        extends SampleOperator<Type>
        implements SparkExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param sampleSize
     */
    public SparkBernoulliSampleOperator(int sampleSize, DataSetType<Type> type) {
        super(sampleSize, type, Methods.BERNOULLI);
    }

    /**
     * Creates a new instance.
     *
     * @param sampleSize
     * @param datasetSize
     */
    public SparkBernoulliSampleOperator(int sampleSize, long datasetSize, DataSetType<Type> type) {
        super(sampleSize, datasetSize, type, Methods.BERNOULLI);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkBernoulliSampleOperator(SampleOperator<Type> that) {
        super(that);
        assert that.getSampleMethod() == Methods.BERNOULLI || that.getSampleMethod() == Methods.ANY;
    }

    @Override
    public void evaluate(ChannelInstance[] inputs,
                         ChannelInstance[] outputs,
                         SparkExecutor sparkExecutor,
                         OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final RddChannel.Instance input = (RddChannel.Instance) inputs[0];
        final RddChannel.Instance output = (RddChannel.Instance) outputs[0];

        double sampleFraction;

        final JavaRDD<Type> inputRdd = input.provideRdd();
        if (datasetSize > 0) //sample size was given as input
            sampleFraction = ((double) sampleSize) / datasetSize;
        else
            sampleFraction = ((double) sampleSize) / inputRdd.cache().count();
        final JavaRDD<Type> outputRdd = inputRdd.sample(false, sampleFraction);
        this.name(outputRdd);

        output.accept(outputRdd, sparkExecutor);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkBernoulliSampleOperator<>(this.sampleSize, this.datasetSize, this.getType());
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(org.qcri.rheem.core.api.Configuration configuration) {
        // NB: This was not measured but is guesswork, adapted from SparkFilterOperator.
        final NestableLoadProfileEstimator mainEstimator = new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(1, 1, .9d, (inputCards, outputCards) -> 700 * inputCards[0] + 500000000L),
                new DefaultLoadEstimator(1, 1, .9d, (inputCards, outputCards) -> 10000),
                new DefaultLoadEstimator(1, 1, .9d, (inputCards, outputCards) -> 0),
                new DefaultLoadEstimator(1, 1, .9d, (inputCards, outputCards) -> 0),
                (in, out) -> 0.23d,
                550
        );

        return Optional.of(mainEstimator);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        if (index == 0) {
            return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
        } else {
            return Collections.singletonList(BroadcastChannel.DESCRIPTOR);
        }
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public boolean isExecutedEagerly() {
        return false;
    }
}
