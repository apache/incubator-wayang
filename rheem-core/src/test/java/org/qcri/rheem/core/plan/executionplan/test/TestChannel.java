package org.qcri.rheem.core.plan.executionplan.test;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.Executor;

/**
 * {@link Channel} implementation that can be used for test purposes.
 */
public class TestChannel extends Channel {

    /**
     * Creates a new instance.
     *
     * @param isReusable whether this instance {@link #isReusable()}
     */
    public TestChannel(boolean isReusable) {
        super(new ChannelDescriptor(TestChannel.class, isReusable, true), null);
    }

    @Override
    public Channel copy() {
        throw new RuntimeException("Not implemented.");
    }


    @Override
    public ChannelInstance createInstance(Executor executor,
                                          OptimizationContext.OperatorContext producerOperatorContext,
                                          int producerOutputIndex) {
        throw new UnsupportedOperationException();
    }

}
