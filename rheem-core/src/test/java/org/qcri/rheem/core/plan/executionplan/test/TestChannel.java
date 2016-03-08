package org.qcri.rheem.core.plan.executionplan.test;

import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.platform.ChannelDescriptor;

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
        super(new ChannelDescriptor(TestChannel.class, isReusable, true, true), null);
    }

    @Override
    public Channel copy() {
        throw new RuntimeException("Not implemented.");
    }


}
