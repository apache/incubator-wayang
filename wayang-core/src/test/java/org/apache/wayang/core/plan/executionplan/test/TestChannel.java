package org.apache.incubator.wayang.core.plan.executionplan.test;

import org.apache.incubator.wayang.core.optimizer.OptimizationContext;
import org.apache.incubator.wayang.core.plan.executionplan.Channel;
import org.apache.incubator.wayang.core.platform.ChannelDescriptor;
import org.apache.incubator.wayang.core.platform.ChannelInstance;
import org.apache.incubator.wayang.core.platform.Executor;

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
