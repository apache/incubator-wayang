package org.qcri.rheem.spark.channels;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;

/**
 * Describes the situation where one {@link JavaRDD} is operated on, producing a further {@link JavaRDD}.
 * <p><i>NB: We might be more specific: Distinguish between cached/uncached and pipelined/aggregated.</i></p>
 */
public class RddChannel extends Channel {

    private static final boolean IS_REUSABLE = true;

    private static final boolean IS_INTERNAL = true;

    protected RddChannel(ExecutionTask producer, int outputIndex) {
        super(producer, outputIndex);
    }

    private RddChannel(RddChannel parent) {
        super(parent);
    }

    @Override
    public boolean isReusable() {
        return IS_REUSABLE;
    }

    @Override
    public boolean isInterStageCapable() {
        return IS_REUSABLE;
    }

    @Override
    public boolean isInterPlatformCapable() {
        return IS_REUSABLE & !IS_INTERNAL;
    }

    @Override
    public RddChannel copy() {
        return new RddChannel(this);
    }

    static class Initializer implements ChannelInitializer {

        @Override
        public Channel setUpOutput(ExecutionTask executionTask, int index) {
            final Channel existingOutputChannel = executionTask.getOutputChannel(index);
            if (existingOutputChannel == null) {
                return new RddChannel(executionTask, index);
            } else if (existingOutputChannel instanceof RddChannel) {
                return existingOutputChannel;
            } else {
                throw new IllegalStateException();
            }
        }

        @Override
        public void setUpInput(Channel channel, ExecutionTask executionTask, int index) {
            assert channel instanceof RddChannel;
            channel.addConsumer(executionTask, index);
        }

        @Override
        public boolean isReusable() {
            return true;
        }

        @Override
        public boolean isInternal() {
            return true;
        }
    }

}
