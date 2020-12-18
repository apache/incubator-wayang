package io.rheem.rheem.spark.channels;

import org.apache.spark.broadcast.Broadcast;
import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.plan.executionplan.Channel;
import io.rheem.rheem.core.plan.rheemplan.OutputSlot;
import io.rheem.rheem.core.platform.AbstractChannelInstance;
import io.rheem.rheem.core.platform.ChannelDescriptor;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.core.platform.Executor;
import io.rheem.rheem.spark.execution.SparkExecutor;

/**
 * {@link Channel} that represents a broadcasted value.
 */
public class BroadcastChannel extends Channel {

    public static final ChannelDescriptor DESCRIPTOR = new ChannelDescriptor(
            BroadcastChannel.class, true, true);

    public BroadcastChannel(ChannelDescriptor descriptor, OutputSlot<?> outputSlot) {
        super(descriptor, outputSlot);
    }

    private BroadcastChannel(BroadcastChannel parent) {
        super(parent);
    }


    @Override
    public BroadcastChannel copy() {
        return new BroadcastChannel(this);
    }

    @Override
    public Instance createInstance(Executor executor,
                                   OptimizationContext.OperatorContext producerOperatorContext,
                                   int producerOutputIndex) {
        return new Instance((SparkExecutor) executor, producerOperatorContext, producerOutputIndex);
    }

    /**
     * {@link ChannelInstance} implementation for {@link BroadcastChannel}s.
     */
    public class Instance extends AbstractChannelInstance {

        private Broadcast<?> broadcast;

        public Instance(SparkExecutor executor, OptimizationContext.OperatorContext producerOperatorContext, int producerOutputIndex) {
            super(executor, producerOperatorContext, producerOutputIndex);
        }

        public void accept(Broadcast broadcast) {
            assert this.broadcast == null : String.format("Broadcast for %s already initialized.", this.getChannel());
            this.broadcast = broadcast;
        }

        @SuppressWarnings("unchecked")
        public Broadcast<?> provideBroadcast() {
            assert this.broadcast != null : String.format("Broadcast for %s not initialized.", this.getChannel());
            return this.broadcast;
        }

        @Override
        protected void doDispose() {
         // TODO: We must not dispose broadcasts for now because they can break lazy computation.
//            this.doSafe(() -> this.broadcast.destroy(false));
        }

        @Override
        public BroadcastChannel getChannel() {
            return BroadcastChannel.this;
        }

    }

}
