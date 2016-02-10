package org.qcri.rheem.spark.channels;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;

/**
 * Describes the situation where one {@link JavaRDD} is operated on, producing a further {@link JavaRDD}.
 * <p><i>NB: We might be more specific: Distinguish between cached/uncached and pipelined/aggregated.</i></p>
 */
public class RddChannel extends Channel {

    protected RddChannel(ExecutionTask producer, int outputIndex) {
        super(producer, outputIndex);
    }

    @Override
    public boolean isReusable() {
        return true;
    }

    static class Initializer implements ChannelInitializer<RddChannel> {

        @Override
        public RddChannel setUpOutput(ExecutionTask executionTask, int index) {
            return new RddChannel(executionTask, index);
        }

        @Override
        public void setUpInput(RddChannel channel, ExecutionTask executionTask, int index) {
            channel.addConsumer(executionTask, index);
        }

        @Override
        public boolean isReusable() {
            return true;
        }
    }
}
