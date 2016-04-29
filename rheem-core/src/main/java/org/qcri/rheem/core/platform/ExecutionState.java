package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.optimizer.enumeration.ExecutionTaskFlow;
import org.qcri.rheem.core.plan.executionplan.Channel;

import java.util.HashMap;
import java.util.Map;

/**
 * Contains a state of the execution of an {@link ExecutionTaskFlow}.
 */
public class ExecutionState {

    /**
     * Keeps track of {@link Channel} cardinalities.
     */
    private final Map<Channel, Long> cardinalities = new HashMap<>();

    /**
     * Keeps track of {@link ChannelInstance}s so as to reuse them among {@link Executor} runs.
     */
    private final Map<Channel, ChannelInstance> channelInstances = new HashMap<>();

    public Map<Channel, Long> getCardinalities() {
        return this.cardinalities;
    }

    public Map<Channel, ChannelInstance> getChannelInstances() {
        return this.channelInstances;
    }

    public void merge(ExecutionState that) {
        if (that != null) {
            this.cardinalities.putAll(that.cardinalities);
            this.channelInstances.putAll(that.channelInstances);
        }
    }
}
