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
public class DummyNonReusableChannel extends Channel {

    public static final ChannelDescriptor DESCRIPTOR = new ChannelDescriptor(
            DummyNonReusableChannel.class,
            false,
            false
    );

    public DummyNonReusableChannel(ChannelDescriptor descriptor, OutputSlot<?> producerSlot) {
        super(descriptor, producerSlot);
        assert DESCRIPTOR == descriptor;
    }

    public DummyNonReusableChannel(Channel original) {
        super(original);
    }

    @Override
    public DummyNonReusableChannel copy() {
        return new DummyNonReusableChannel(this);
    }

    @Override
    public ChannelInstance createInstance(Executor executor,
                                          OptimizationContext.OperatorContext producerOperatorContext,
                                          int producerOutputIndex) {
        throw new UnsupportedOperationException();
    }
}
