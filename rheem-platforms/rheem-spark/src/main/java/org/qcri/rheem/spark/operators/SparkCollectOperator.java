package org.qcri.rheem.spark.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.platform.JavaPlatform;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.execution.SparkExecutor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Converts a {@link RddChannel} into a {@link CollectionChannel} of the {@link JavaPlatform}.
 */
public class SparkCollectOperator<Type>
        extends UnaryToUnaryOperator<Type, Type>
        implements SparkExecutionOperator {

    public SparkCollectOperator(DataSetType<Type> type) {
        super(type, type, false);
    }

    @Override
    public void evaluate(ChannelInstance[] inputs,
                         ChannelInstance[] outputs,
                         SparkExecutor sparkExecutor,
                         OptimizationContext.OperatorContext operatorContext) {
        RddChannel.Instance input = (RddChannel.Instance) inputs[0];
        CollectionChannel.Instance output = (CollectionChannel.Instance) outputs[0];

        @SuppressWarnings("unchecked")
        final List<Type> collectedRdd = (List<Type>) input.provideRdd().collect();
        output.accept(collectedRdd);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
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
    public Optional<LoadProfileEstimator<ExecutionOperator>> createLoadProfileEstimator(Configuration configuration) {
        final String specification = configuration.getStringProperty("rheem.spark.collect.load");
        final NestableLoadProfileEstimator<ExecutionOperator> mainEstimator =
                LoadProfileEstimators.createFromJuelSpecification(specification);
        return Optional.of(mainEstimator);
    }

    @Override
    public boolean isExecutedEagerly() {
        return true;
    }
}
