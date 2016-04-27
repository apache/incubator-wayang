package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * TODO
 */
public interface ChannelManager {

    /**
     * Provides a {@link ChannelInitializer} for the given {@link Channel} class.
     *
     * @param channelDescriptor describes a type of {@link Channel}
     * @return the {@link ChannelInitializer}
     */
    ChannelInitializer getChannelInitializer(ChannelDescriptor channelDescriptor);

    /**
     * @return {@link ChannelConversion}s that this instance provides
     */
    Collection<ChannelConversion> getChannelConversions();

    /**
     * Exchange the given {@link Channel} with one that is capable of inter-stage processing.
     *
     * @param channel the {@link Channel} to be exchanged
     * @return whether the exchanged succeeded
     */
    boolean exchangeWithInterstageCapable(Channel channel);

    ChannelDescriptor getInternalChannelDescriptor(boolean isRequestReusable);

    Map<ChannelDescriptor, Channel> setUpSourceSide(
            Junction junction,
            List<ChannelDescriptor> preferredChannelDescriptors,
            OptimizationContext optimizationContext);

    void setUpTargetSide(Junction junction, int targetIndex, Channel externalChannel, OptimizationContext optimizationContext);

}
