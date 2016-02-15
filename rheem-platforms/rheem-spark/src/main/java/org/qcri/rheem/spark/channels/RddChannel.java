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
            final Channel existingOutputChannel = executionTask.getOutputChannel(index);
            if (existingOutputChannel == null) {
                return new RddChannel(executionTask, index);
            } else if (existingOutputChannel instanceof RddChannel) {
                return (RddChannel) existingOutputChannel;
            } else {
                throw new IllegalStateException();
            }
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

    public static class Executor implements ChannelExecutor {

        private final boolean isCache;

        private JavaRDDLike rdd;

        public Executor(boolean isCache) {
            this.isCache = isCache;
        }

        @Override
        public void acceptRdd(JavaRDDLike rdd) {
            this.rdd = rdd;
            if (this.isCache) {
                ((JavaRDD) this.rdd).cache();
            }
        }

        @Override
        public JavaRDDLike provideRdd() {
            return this.rdd;
        }

        @Override
        public void dispose() {
            ((JavaRDD) this.rdd).unpersist();
        }
    }
}
