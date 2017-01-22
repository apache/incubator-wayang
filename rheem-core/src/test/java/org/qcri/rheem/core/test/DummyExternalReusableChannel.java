package org.qcri.rheem.core.test;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.Executor;

/**
 * Dummy {@link Channel}.
 */
public class DummyExternalReusableChannel extends Channel {

    public static final ChannelDescriptor DESCRIPTOR = new ChannelDescriptor(
            DummyExternalReusableChannel.class,
            true,
            true
    );

    public DummyExternalReusableChannel(ChannelDescriptor descriptor, OutputSlot<?> producerSlot) {
        super(descriptor, producerSlot);
        assert DESCRIPTOR == descriptor;
    }

    public DummyExternalReusableChannel(Channel original) {
        super(original);
    }

    @Override
    public DummyExternalReusableChannel copy() {
        return new DummyExternalReusableChannel(this);
    }

    @Override
    public ChannelInstance createInstance(Executor executor,
                                          OptimizationContext.OperatorContext producerOperatorContext,
                                          int producerOutputIndex) {
        throw new UnsupportedOperationException();
    }
}
