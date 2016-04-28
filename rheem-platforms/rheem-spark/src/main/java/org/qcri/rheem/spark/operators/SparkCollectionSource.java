package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.basic.operators.CollectionSource;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

import java.util.*;

/**
 * Provides a {@link Collection} to a Spark job. Can also be used to convert {@link CollectionChannel}s of the
 * {@link JavaPlatform} into {@link RddChannel}s.
 */
public class SparkCollectionSource<Type> extends CollectionSource<Type> implements SparkExecutionOperator {

    /**
     * Create a new instance to convert a {@link CollectionChannel} into a {@link RddChannel}.
     */
    public SparkCollectionSource(DataSetType<Type> type) {
        this(null, type);
    }

    /**
     * Create a new instance to use a {@code collection} in a {@link RheemPlan}.
     */
    public SparkCollectionSource(Collection<Type> collection, DataSetType<Type> type) {
        super(collection, type);
    }

    @Override
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length <= 1;
        assert outputs.length == this.getNumOutputs();

        final Collection<Type> collection;
        if (this.collection != null) {
            collection = this.collection;
        } else {
            final CollectionChannel.Instance input = (CollectionChannel.Instance) inputs[0];
            collection = input.provideCollection();
        }
        final List<Type> list = RheemCollections.asList(collection);

        final RddChannel.Instance output = (RddChannel.Instance) outputs[0];
        final JavaRDD<Type> rdd = sparkExecutor.sc.parallelize(list);
        output.accept(rdd, sparkExecutor);
    }

    private List<Type> getCollectionAsList() {
        final Collection<Type> collection = this.getCollection();
        if (this.collection instanceof List) {
            return (List<Type>) this.collection;
        }
        return new ArrayList<>(collection);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkCollectionSource<>(this.getCollection(), this.getType());
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(org.qcri.rheem.core.api.Configuration configuration) {
        final NestableLoadProfileEstimator mainEstimator = new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(0, 1, .9d, (inputCards, outputCards) -> 1500 * outputCards[0] + 2000L),
                new DefaultLoadEstimator(0, 1, .9d, (inputCards, outputCards) -> 100 * outputCards[0] + 2000),
                new DefaultLoadEstimator(0, 1, .9d, (inputCards, outputCards) -> 5 * outputCards[0] + 2000),
                new DefaultLoadEstimator(0, 1, .9d, (inputCards, outputCards) -> 0),
                0.75d,
                2000
        );

        return Optional.of(mainEstimator);
    }

    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(CollectionChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }
}
