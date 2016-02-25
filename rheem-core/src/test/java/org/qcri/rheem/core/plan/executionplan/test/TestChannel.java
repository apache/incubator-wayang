package org.qcri.rheem.core.plan.executionplan.test;

import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.platform.ChannelDescriptor;

/**
 * {@link Channel} implementation that can be used for test purposes.
 */
public class TestChannel extends Channel {

    private final boolean isReusable;

    private final boolean isInternal;

    /**
     * Creates a new instance.
     *
     * @param isReusable whether this instance {@link #isReusable()}
     * @see Channel#Channel(ChannelDescriptor, ExecutionTask, int)
     */
    public TestChannel(ExecutionTask producer, int outputIndex, boolean isReusable) {
        super(null, producer, outputIndex);
        this.isReusable = isReusable;
        this.isInternal = true;
    }

    @Override
    public boolean isReusable() {
        return this.isReusable;
    }

    @Override
    public boolean isInterStageCapable() {
        return this.isReusable;
    }

    @Override
    public boolean isInterPlatformCapable() {
        return this.isReusable & !this.isInternal;
    }
    @Override
    public Channel copy() {
        throw new RuntimeException("Not implemented.");
    }
}
