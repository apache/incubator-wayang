package io.rheem.rheem.spark.operators;

import org.apache.commons.lang3.Validate;
import org.apache.spark.api.java.JavaRDD;
import io.rheem.rheem.core.api.Configuration;
import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.optimizer.cardinality.CardinalityEstimator;
import io.rheem.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import io.rheem.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import io.rheem.rheem.core.platform.ChannelDescriptor;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.core.platform.lineage.ExecutionLineageNode;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.core.util.Tuple;
import io.rheem.rheem.spark.channels.RddChannel;
import io.rheem.rheem.spark.execution.SparkExecutor;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Converts an uncached {@link RddChannel} into a cached {@link RddChannel}.
 */
public class SparkCacheOperator<Type>
        extends UnaryToUnaryOperator<Type, Type>
        implements SparkExecutionOperator {

    public SparkCacheOperator(DataSetType<Type> type) {
        super(type, type, false);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        RddChannel.Instance input = (RddChannel.Instance) inputs[0];
        final JavaRDD<Object> rdd = input.provideRdd();
        final JavaRDD<Object> cachedRdd = rdd.cache();
        cachedRdd.foreachPartition(iterator -> {
        });

        RddChannel.Instance output = (RddChannel.Instance) outputs[0];
        output.accept(cachedRdd, sparkExecutor);

        return ExecutionOperator.modelQuasiEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(RddChannel.CACHED_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return true;
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, 0, outputIndex);
        return Optional.of(new DefaultCardinalityEstimator(1d, 1, this.isSupportingBroadcastInputs(),
                inputCards -> inputCards[0]));
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.spark.cache.load";
    }

}
