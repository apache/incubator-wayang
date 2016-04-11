package org.qcri.rheem.spark.channels;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.util.Tuple;

/**
 * Describes the situation where one {@link JavaRDD} is operated on, producing a further {@link JavaRDD}.
 * <p><i>NB: We might be more specific: Distinguish between cached/uncached and pipelined/aggregated.</i></p>
 */
public class RddChannel extends Channel {

    private static final boolean IS_REUSABLE = true;

    private static final boolean IS_INTERNAL = true;

    public static final ChannelDescriptor DESCRIPTOR = new ChannelDescriptor(RddChannel.class, IS_REUSABLE, IS_REUSABLE, !IS_INTERNAL && IS_REUSABLE);

    public RddChannel(ChannelDescriptor descriptor, OutputSlot<?> outputSlot) {
        super(descriptor, outputSlot);
        assert descriptor == DESCRIPTOR;
    }

    private RddChannel(RddChannel parent) {
        super(parent);
    }

    @Override
    public RddChannel copy() {
        return new RddChannel(this);
    }

    /**
     * {@link SparkChannelInitializer} for the {@link RddChannel}.
     */
    static class Initializer implements SparkChannelInitializer {

              @Override
        public RddChannel provideRddChannel(Channel channel, OptimizationContext optimizationContext) {
            return (RddChannel) channel;
        }

        @Override
        public Tuple<Channel, Channel> setUpOutput(ChannelDescriptor descriptor, OutputSlot<?> outputSlot, OptimizationContext optimizationContext) {
            final RddChannel rddChannel = new RddChannel(descriptor, outputSlot);
            return new Tuple<>(rddChannel, rddChannel);
        }

        @Override
        public Channel setUpOutput(ChannelDescriptor descriptor, Channel source, OptimizationContext optimizationContext) {
            assert descriptor == RddChannel.DESCRIPTOR;
            return this.createRddChannel(source, optimizationContext);
        }
    }

}
