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
        this.channelInstance.noteObtainedReference();
    }

    @Override
    protected <T> T accept(T accumulator, Aggregator<T> aggregator) {
        return aggregator.aggregate(accumulator, this);
    }


    @Override
    protected void markAsExecuted() {
        super.markAsExecuted();
        assert !this.channelInstance.wasProduced();
        this.channelInstance.markProduced();
        this.channelInstance.noteDiscardedReference(false);
    }

    @Override
    public String toString() {
        return "ChannelLineageNode[" + channelInstance + ']';
    }

    /**
     * Retrieve the encapsulated {@link ChannelInstance}.
     *
     * @return the {@link ChannelInstance}
     */
    public ChannelInstance getChannelInstance() {
        return this.channelInstance;
    }
}
