package org.qcri.rheem.spark.channels;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.spark.operators.SparkBroadcastOperator;

/**
 * {@link Channel} that represents a broadcasted value.
 */
public class BroadcastChannel extends Channel {

    private static final boolean IS_REUSABLE = true;

    private static final boolean IS_INTERNAL = true;

    public static final ChannelDescriptor DESCRIPTOR = new ChannelDescriptor(
            BroadcastChannel.class, IS_REUSABLE, IS_REUSABLE, IS_INTERNAL);

    protected BroadcastChannel(ChannelDescriptor descriptor) {
        super(descriptor);
    }

    private BroadcastChannel(BroadcastChannel parent) {
        super(parent);
    }


    @Override
    public BroadcastChannel copy() {
        return new BroadcastChannel(this);
    }

    /**
     * {@link SparkChannelInitializer} for the {@link BroadcastChannel}.
     */
    public static class Initializer implements SparkChannelInitializer {

        @Override
        public RddChannel provideRddChannel(Channel channel, OptimizationContext optimizationContext) {
            throw new UnsupportedOperationException("Not yet implemented.");
        }

        @Override
        public Tuple<Channel, Channel> setUpOutput(ChannelDescriptor descriptor, OutputSlot<?> outputSlot, OptimizationContext optimizationContext) {
            throw new UnsupportedOperationException("Not yet implemented.");
        }

        @Override
        public Channel setUpOutput(ChannelDescriptor descriptor, Channel source, OptimizationContext optimizationContext) {
            assert descriptor == BroadcastChannel.DESCRIPTOR;

            // Set up an intermediate Channel at first.
            final RddChannel rddChannel = this.createRddChannel(source, optimizationContext);

            // Next, broadcast the data.
            final ExecutionTask broadcastTask = rddChannel.getConsumers().stream()
                    .filter(consumer -> consumer.getOperator() instanceof SparkBroadcastOperator)
                    .findAny()
                    .orElseGet(() -> {
                        SparkBroadcastOperator sbo = new SparkBroadcastOperator(source.getDataSetType());
                        ExecutionTask task = new ExecutionTask(sbo);
                        rddChannel.addConsumer(task, 0);
                        return task;
                    });

            // Finally, get or create the BroadcastChannel.
            if (broadcastTask.getOutputChannel(0) != null) {
                assert broadcastTask.getOutputChannel(0) instanceof BroadcastChannel;
                return broadcastTask.getOutputChannel(0);
            } else {
                final BroadcastChannel broadcastChannel = new BroadcastChannel(descriptor);
                broadcastTask.setOutputChannel(0, broadcastChannel);
                return broadcastChannel;
            }
        }
    }

}
