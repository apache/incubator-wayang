package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.util.Tuple;

import java.util.List;

/**
 * TODO
 */
public interface ChannelManager {

    /**
     * TODO
     */
    boolean connect(ExecutionTask sourceTask, int outputIndex, List<Tuple<ExecutionTask, Integer>> targetDescriptors);

    /**
     * Provides a {@link ChannelInitializer} for the given {@link Channel} class.
     *
     * @param channelClass the {@link Channel} class
     * @return the {@link ChannelInitializer}
     */
    ChannelInitializer getChannelInitializer(Class<? extends Channel> channelClass);

    /**
     * Exchange the given {@link Channel} with one that is capable of inter-stage processing.
     *
     * @param channel the {@link Channel} to be exchanged
     * @return whether the exchanged succeeded
     */
    boolean exchangeWithInterstageCapable(Channel channel);

}
