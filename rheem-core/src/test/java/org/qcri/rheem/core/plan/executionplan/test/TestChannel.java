package org.qcri.rheem.core.plan.executionplan.test;

import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;

/**
 * {@link Channel} implementation that can be used for test purposes.
 */
public class TestChannel extends Channel {

    private final boolean isReusable;

    /**
     * Creates a new instance.
     *
     * @param isReusable whether this instance {@link #isReusable()}
     * @see Channel#Channel(ExecutionTask, int)
     */
    public TestChannel(ExecutionTask producer, int outputIndex, boolean isReusable) {
        super(producer, outputIndex);
        this.isReusable = isReusable;
    }

    @Override
    public boolean isReusable() {
        return this.isReusable;
    }

}
