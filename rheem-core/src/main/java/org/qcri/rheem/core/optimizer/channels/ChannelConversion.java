package org.qcri.rheem.core.optimizer.channels;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.DefaultOptimizationContext;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.platform.ChannelDescriptor;

import java.util.Collection;
import java.util.Collections;

/**
 * Can convert a given {@link Channel} to another {@link Channel}.
 */
public abstract class ChannelConversion {

    private final ChannelDescriptor sourceChannelDescriptor;

    private final ChannelDescriptor targetChannelDescriptor;

    public ChannelConversion(ChannelDescriptor sourceChannelDescriptor, ChannelDescriptor targetChannelDescriptor) {
        this.sourceChannelDescriptor = sourceChannelDescriptor;
        this.targetChannelDescriptor = targetChannelDescriptor;
    }

    /**
     * Converts the given {@code sourceChannel} into a new {@link Channel} according to {@link #targetChannelDescriptor}.
     *
     * @param sourceChannel the {@link Channel} to be converted
     * @param configuration can provide additional information for setting up {@link Channel}s etc.
     * @return the newly created {@link Channel}
     */
    public Channel convert(final Channel sourceChannel, Configuration configuration) {
        return this.convert(sourceChannel, configuration, Collections.emptyList(), null);
    }

    /**
     * Converts the given {@code sourceChannel} into a new {@link Channel} according to {@link #targetChannelDescriptor}.
     *
     * @param sourceChannel        the {@link Channel} to be converted
     * @param configuration        can provide additional information for setting up {@link Channel}s etc.
     * @param optimizationContexts to which estimates of the newly added {@link Operator} should be added
     * @param cardinality          optional {@link CardinalityEstimate} of the {@link Channel}
     * @return the newly created {@link Channel}
     */
    public abstract Channel convert(final Channel sourceChannel,
                                    final Configuration configuration,
                                    final Collection<OptimizationContext> optimizationContexts,
                                    final CardinalityEstimate cardinality);

    public ChannelDescriptor getSourceChannelDescriptor() {
        return this.sourceChannelDescriptor;
    }

    public ChannelDescriptor getTargetChannelDescriptor() {
        return this.targetChannelDescriptor;
    }

    /**
     * Estimate the required costs to carry out the conversion for a given {@code cardinality}.
     *
     * @param cardinality   the {@link CardinalityEstimate} of data to be converted
     * @param numExecutions expected number of executions of this instance
     * @param configuration provides estimators
     * @return the cost estimate
     * @see #estimateConversionCost(CardinalityEstimate, int, OptimizationContext)
     */
    public ProbabilisticDoubleInterval estimateConversionCost(CardinalityEstimate cardinality,
                                                              int numExecutions,
                                                              Configuration configuration) {
        return this.estimateConversionCost(cardinality, numExecutions, new DefaultOptimizationContext(configuration));
    }

    /**
     * Estimate the required cost to carry out the conversion for a given {@code cardinality}.
     *
     * @param cardinality         the {@link CardinalityEstimate} of data to be converted
     * @param numExecutions       expected number of executions of this instance
     * @param optimizationContext provides a {@link Configuration} and keeps around generated optimization information
     * @return the cost estimate
     */
    public abstract ProbabilisticDoubleInterval estimateConversionCost(CardinalityEstimate cardinality,
                                                                       int numExecutions,
                                                                       OptimizationContext optimizationContext);

    @Override
    public String toString() {
        return String.format("%s[%s->%s]",
                this.getClass().getSimpleName(),
                this.getSourceChannelDescriptor().getChannelClass().getSimpleName(),
                this.getTargetChannelDescriptor().getChannelClass().getSimpleName()
        );
    }
}
