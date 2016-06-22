package org.qcri.rheem.core.optimizer.channels;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.platform.ChannelDescriptor;

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
    public abstract Channel convert(final Channel sourceChannel, Configuration configuration);

    public ChannelDescriptor getSourceChannelDescriptor() {
        return this.sourceChannelDescriptor;
    }

    public ChannelDescriptor getTargetChannelDescriptor() {
        return this.targetChannelDescriptor;
    }

    /**
     * Estimate the required time to carry out the conversion for a given {@code cardinality}.
     *
     * @param cardinality   the {@link CardinalityEstimate} of data to be converted
     * @param numExecutions expected number of executions of this instance
     * @param configuration provides estimators
     * @return the {@link TimeEstimate}
     * @see #estimateConversionTime(CardinalityEstimate, int, OptimizationContext)
     */
    public TimeEstimate estimateConversionTime(CardinalityEstimate cardinality, int numExecutions, Configuration configuration) {
        return this.estimateConversionTime(cardinality, numExecutions, new OptimizationContext(configuration));
    }

    /**
     * Estimate the required time to carry out the conversion for a given {@code cardinality}.
     *
     * @param cardinality   the {@link CardinalityEstimate} of data to be converted
     * @param numExecutions expected number of executions of this instance
     * @param optimizationContext provides a {@link Configuration} and keeps around generated optimization information
     * @return the {@link TimeEstimate}
     */
    public abstract TimeEstimate estimateConversionTime(CardinalityEstimate cardinality,
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
