package io.rheem.rheem.core.test;

import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.plan.executionplan.Channel;
import io.rheem.rheem.core.plan.rheemplan.OutputSlot;
import io.rheem.rheem.core.platform.ChannelDescriptor;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.core.platform.Executor;

/**
 * Dummy {@link Channel}.
 */
public class DummyReusableChannel extends Channel {

    public static final ChannelDescriptor DESCRIPTOR = new ChannelDescriptor(
            DummyReusableChannel.class,
            true,
            true
    );

    public DummyReusableChannel(ChannelDescriptor descriptor, OutputSlot<?> producerSlot) {
        super(descriptor, producerSlot);
        assert DESCRIPTOR == descriptor;
    }

    public DummyReusableChannel(DummyReusableChannel dummyReusableChannel) {
        super(dummyReusableChannel);
    }

    @Override
    public Channel copy() {
        return new DummyReusableChannel(this);
    }

    @Override
    public ChannelInstance createInstance(Executor executor,
                                          OptimizationContext.OperatorContext producerOperatorContext,
                                          int producerOutputIndex) {
        throw new UnsupportedOperationException();
    }
}
