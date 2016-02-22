package org.qcri.rheem.spark.channels;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.spark.operators.SparkBroadcastOperator;
import org.qcri.rheem.spark.platform.SparkPlatform;

/**
 * {@link Channel} that represents a broadcasted value.
 */
public class BroadcastChannel extends Channel {

    private static final boolean IS_REUSABLE = true;

    private static final boolean IS_INTERNAL = true;

    protected BroadcastChannel(ExecutionTask producer, int outputIndex, CardinalityEstimate cardinalityEstimate) {
        super(producer, outputIndex, cardinalityEstimate);
    }

    private BroadcastChannel(BroadcastChannel parent) {
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
    public BroadcastChannel copy() {
        return new BroadcastChannel(this);
    }

    public static class Initializer implements ChannelInitializer {

        @Override
        public Channel setUpOutput(ExecutionTask executionTask, int index) {
            assert executionTask.getOperator().getPlatform() == SparkPlatform.getInstance();

            // Set up an intermediate Channel at first.
            final Platform platform = executionTask.getOperator().getPlatform();
            final ChannelInitializer rddChannelInitializer = platform.getChannelManager().getChannelInitializer(RddChannel.class);
            final Channel rddChannel = rddChannelInitializer.setUpOutput(executionTask, index);

            // Next, broadcast the data.
            final ExecutionTask broadcastTask = rddChannel.getConsumers().stream()
                    .filter(consumer -> consumer.getOperator() instanceof SparkBroadcastOperator)
                    .findAny()
                    .orElseGet(() -> {
                        SparkBroadcastOperator sbo = new SparkBroadcastOperator(executionTask.getOperator().getOutput(index).getType());
                        sbo.getInput(0).setCardinalityEstimate(rddChannel.getCardinalityEstimate());
                        sbo.getOutput(0).setCardinalityEstimate(rddChannel.getCardinalityEstimate());
                        ExecutionTask task = new ExecutionTask(sbo);
                        rddChannel.addConsumer(task, 0);
                        return task;
                    });

            // Finally, get or create the BroadcastChannel.
            if (broadcastTask.getOutputChannel(0) != null) {
                assert broadcastTask.getOutputChannel(0) instanceof BroadcastChannel;
                return broadcastTask.getOutputChannel(0);
            } else {
                return new BroadcastChannel(broadcastTask, 0, rddChannel.getCardinalityEstimate());
            }
        }

        @Override
        public void setUpInput(Channel channel, ExecutionTask executionTask, int index) {
            assert channel instanceof BroadcastChannel;
            channel.addConsumer(executionTask, index);
        }

        @Override
        public boolean isReusable() {
            return IS_REUSABLE;
        }

        @Override
        public boolean isInternal() {
            return IS_INTERNAL;
        }
    }

}
