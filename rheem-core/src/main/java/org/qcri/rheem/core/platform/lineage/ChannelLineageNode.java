package org.qcri.rheem.core.platform.lineage;

import org.qcri.rheem.core.platform.ChannelInstance;

/**
 * Encapsulates a {@link ChannelInstance} in the lazy execution lineage.
 */
public class ChannelLineageNode extends LazyExecutionLineageNode {


    /**
     * The encapsulated {@link ChannelInstance}.
     */
    private final ChannelInstance channelInstance;

    public ChannelLineageNode(final ChannelInstance channelInstance) {
        assert !channelInstance.wasProduced();
        this.channelInstance = channelInstance;
    }

    @Override
    protected <T> T accept(T accumulator, Aggregator<T> aggregator) {
        return aggregator.aggregate(accumulator, this.channelInstance);
    }

    @Override
    protected void markAsExecuted() {
        super.markAsExecuted();
        assert !this.channelInstance.wasProduced();
        this.channelInstance.markProduced();
    }
}
