package org.qcri.rheem.core.optimizer.channels;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.util.Tuple;

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

    /**
     * Describes the conversion being done by this instance.
     *
     * @return a {@link Tuple} <code>(source channel, target channel)</code>
     */
    public Tuple<ChannelDescriptor, ChannelDescriptor> getConversionKey() {
        return new Tuple<>(this.getSourceChannelDescriptor(), this.getTargetChannelDescriptor());
    }

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
     * @param configuration provides estimators
     * @return the {@link TimeEstimate}
     */
    public abstract TimeEstimate estimateConversionTime(CardinalityEstimate cardinality, Configuration configuration);

    @Override
    public String toString() {
        return String.format("%s[%s->%s]",
                this.getClass().getSimpleName(),
                this.getSourceChannelDescriptor().getChannelClass().getSimpleName(),
                this.getTargetChannelDescriptor().getChannelClass().getSimpleName()
        );
    }
}
