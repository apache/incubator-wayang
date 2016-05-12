package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.optimizer.enumeration.ExecutionTaskFlow;
import org.qcri.rheem.core.plan.executionplan.Channel;

import java.util.Map;

/**
 * Contains a state of the execution of an {@link ExecutionTaskFlow}.
 */
public interface ExecutionState {

    /**
     * Register a {@link ChannelInstance}.
     *
     * @param channelInstance that should be registered
     */
    void register(ChannelInstance channelInstance);

    /**
     * Obtain a previously registered {@link ChannelInstance}.
     *
     * @param channel the {@link Channel} of the {@link ChannelInstance}
     * @return the {@link ChannelInstance} or {@code null} if none
     */
    ChannelInstance getChannelInstance(Channel channel);

    /**
     * TODO: What are the cardinalities good for if they cannot be put into context of loops.
     * @return
     */
    Map<Channel,Long> getCardinalities();
}
