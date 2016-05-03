package org.qcri.rheem.spark.channels;

import org.apache.spark.broadcast.Broadcast;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.AbstractChannelInstance;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;

/**
 * {@link Channel} that represents a broadcasted value.
 */
public class BroadcastChannel extends Channel {

    private static final boolean IS_REUSABLE = true;

    private static final boolean IS_INTERNAL = true;

    public static final ChannelDescriptor DESCRIPTOR = new ChannelDescriptor(
            BroadcastChannel.class, IS_REUSABLE, IS_REUSABLE, !IS_INTERNAL && IS_REUSABLE);

    public BroadcastChannel(ChannelDescriptor descriptor, OutputSlot<?> outputSlot) {
        super(descriptor, outputSlot);
    }

    private BroadcastChannel(BroadcastChannel parent) {
        super(parent);
    }


    @Override
    public BroadcastChannel copy() {
        return new BroadcastChannel(this);
    }

    @Override
    public ChannelInstance createInstance() {
        return new Instance();
    }

    /**
     * {@link ChannelInstance} implementation for {@link BroadcastChannel}s.
     */
    public class Instance extends AbstractChannelInstance {

        private Broadcast<?> broadcast;

        public void accept(Broadcast broadcast) {
            assert this.broadcast == null : String.format("Broadcast for %s already initialized.", this.getChannel());
            this.broadcast = broadcast;
        }

        @SuppressWarnings("unchecked")
        public Broadcast<?> provideBroadcast() {
            assert this.broadcast != null : String.format("Broadcast for %s not initialized.", this.getChannel());
            return this.broadcast;
        }

        @Override
        public void tryToRelease() {
            this.broadcast.destroy(false);
        }

        @Override
        public BroadcastChannel getChannel() {
            return BroadcastChannel.this;
        }

    }

}
