package io.rheem.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import io.rheem.rheem.basic.operators.SampleOperator;
import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.rheem.core.platform.ChannelDescriptor;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.core.platform.lineage.ExecutionLineageNode;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.core.util.Tuple;
import io.rheem.rheem.spark.channels.BroadcastChannel;
import io.rheem.rheem.spark.channels.RddChannel;
import io.rheem.rheem.spark.execution.SparkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.IntUnaryOperator;
import java.util.function.LongUnaryOperator;


/**
 * Spark implementation of the {@link SparkBernoulliSampleOperator}.
 */
public class SparkBernoulliSampleOperator<Type>
        extends SampleOperator<Type>
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     */
    public SparkBernoulliSampleOperator(IntUnaryOperator sampleSizeFunction, DataSetType<Type> type, LongUnaryOperator seedFunction) {
        super(sampleSizeFunction, type, Methods.BERNOULLI, seedFunction);
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
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final RddChannel.Instance input = (RddChannel.Instance) inputs[0];
        final RddChannel.Instance output = (RddChannel.Instance) outputs[0];


        final JavaRDD<Type> inputRdd = input.provideRdd();
        long datasetSize = this.isDataSetSizeKnown() ? this.getDatasetSize() : inputRdd.count();
        int sampleSize = this.getSampleSize(operatorContext);
        long seed = this.getSeed(operatorContext);

        double sampleFraction = ((double) sampleSize) / datasetSize;
        final JavaRDD<Type> outputRdd = inputRdd.sample(false, sampleFraction, seed);
        this.name(outputRdd);

        output.accept(outputRdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkBernoulliSampleOperator<>(this);
    }


    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.spark.bernoulli-sample.load";
    }


    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        if (index == 0) {
            return this.isDataSetSizeKnown() ?
                    Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR) :
                    Collections.singletonList(RddChannel.CACHED_DESCRIPTOR);
        } else {
            return Collections.singletonList(BroadcastChannel.DESCRIPTOR);
        }
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return true;
    }
}
