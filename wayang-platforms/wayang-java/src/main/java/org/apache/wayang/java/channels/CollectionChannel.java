package org.apache.wayang.java.channels;

import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.platform.AbstractChannelInstance;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.java.operators.JavaExecutionOperator;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * {@link Channel} between two {@link JavaExecutionOperator}s using an intermediate {@link Collection}.
 */
public class CollectionChannel extends Channel {

    public static final ChannelDescriptor DESCRIPTOR = new ChannelDescriptor(CollectionChannel.class, true, true);

    public CollectionChannel(ChannelDescriptor channelDescriptor, OutputSlot<?> outputSlot) {
        super(channelDescriptor, outputSlot);
        assert channelDescriptor == DESCRIPTOR;
    }

    private CollectionChannel(CollectionChannel parent) {
        super(parent);
    }

    @Override
    public CollectionChannel copy() {
        return new CollectionChannel(this);
    }

    @Override
    public Instance createInstance(Executor executor,
                                   OptimizationContext.OperatorContext producerOperatorContext,
                                   int producerOutputIndex) {
        return new Instance(executor, producerOperatorContext, producerOutputIndex);
    }

    /**
     * {@link JavaChannelInstance} implementation for the {@link CollectionChannel}.
     */
    public class Instance extends AbstractChannelInstance implements JavaChannelInstance {

        private Collection<?> collection;

        public Instance(Executor executor, OptimizationContext.OperatorContext producerOperatorContext, int producerOutputIndex) {
            super(executor, producerOperatorContext, producerOutputIndex);
        }

        public void accept(Collection<?> collection) {
            this.collection = collection;
            this.setMeasuredCardinality(this.collection.size());
        }

        @SuppressWarnings("unchecked")
        public <T> Collection<T> provideCollection() {
            return (Collection<T>) this.collection;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Stream<T> provideStream() {
            return (Stream<T>) this.collection.stream();
        }

        @Override
        public Channel getChannel() {
            return CollectionChannel.this;
        }

        @Override
        protected void doDispose() {
            logger.debug("Free {}.", this);
            this.collection = null;
        }

    }
}
