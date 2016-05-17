package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.optimizer.enumeration.ExecutionTaskFlow;
import org.qcri.rheem.core.plan.executionplan.Channel;

import java.util.Map;
import java.util.OptionalLong;

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

    // TODO: Handle cardinalities inside of loops.

    /**
     * Registers a measured cardinality.
     *
     * @param channel     for that the cardinality has been measured
     * @param cardinality that has been measured
     */
    void addCardinalityMeasurement(Channel channel, long cardinality);

    /**
     * Obtains a measured cardinality.
     *
     * @param channel whose cardinality measurement is requested
     * @return an {@link OptionalLong} that contains a value if there is a measurement for the given {@link Channel}
     * or one of its siblings
     */
    OptionalLong getCardinalityMeasurement(Channel channel);

    /**
     * Get all registered cardinality measurements. Should be used for reading only.
     *
     * @return all cardinality measurements
     */
    Map<Channel, Long> getCardinalityMeasurements();
}
