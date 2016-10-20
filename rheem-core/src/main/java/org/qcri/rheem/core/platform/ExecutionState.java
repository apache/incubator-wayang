package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.optimizer.enumeration.ExecutionTaskFlow;
import org.qcri.rheem.core.plan.executionplan.Channel;

import java.util.Collection;

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
     * Registers a measured cardinality.
     *
     * @param channelInstance the {@link ChannelInstance} for that there is a measured cardinality
     */
    void addCardinalityMeasurement(ChannelInstance channelInstance);

    /**
     * Retrieve previously registered cardinality measurements
     *
     * @return {@link ChannelInstance}s that contain cardinality measurements
     */
    Collection<ChannelInstance> getCardinalityMeasurements();


    /**
     * Stores a {@link PartialExecution}.
     *
     * @param partialExecution to be stored
     */
    void add(PartialExecution partialExecution);

    /**
     * Retrieves previously stored {@link PartialExecution}s.
     *
     * @return the {@link PartialExecution}s
     */
    Collection<PartialExecution> getPartialExecutions();
}
