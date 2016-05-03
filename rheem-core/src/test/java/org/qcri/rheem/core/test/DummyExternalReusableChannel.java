package org.qcri.rheem.core.test;

import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;

/**
 * Dummy {@link Channel}.
 */
public class DummyExternalReusableChannel extends Channel {

    public static final ChannelDescriptor DESCRIPTOR = new ChannelDescriptor(
            DummyExternalReusableChannel.class,
            true,
            true,
            true
    );

    public DummyExternalReusableChannel(ChannelDescriptor descriptor, OutputSlot<?> producerSlot) {
        super(descriptor, producerSlot);
        assert DESCRIPTOR == descriptor;
    }

    @Override
    public Channel copy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelInstance createInstance() {
        throw new UnsupportedOperationException();
    }
}
