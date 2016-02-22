package org.qcri.rheem.java.channels;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * {@link Channel} between two {@link JavaExecutionOperator}s using a {@link Stream}.
 */
public class StreamChannel extends Channel {

    private static final boolean IS_REUSABLE = false;

    private static final boolean IS_INTERNAL = true;

    protected StreamChannel(ExecutionTask producer, int outputIndex) {
        super(producer, outputIndex);
    }

    private StreamChannel(StreamChannel parent) {
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
    public StreamChannel copy() {
        return new StreamChannel(this);
    }

    public Channel exchangeWith(ChannelInitializer channelInitializer) {
        final ExecutionTask producer = this.producer;
        final int outputIndex = this.producer.removeOutputChannel(this);
        final Channel replacementChannel = channelInitializer.setUpOutput(producer, outputIndex);

        for (ExecutionTask consumer : this.consumers) {
            int inputIndex = consumer.removeInputChannel(this);
            channelInitializer.setUpInput(replacementChannel, consumer, inputIndex);
        }

        if (this.isMarkedForInstrumentation()) {
            replacementChannel.markForInstrumentation();
        }
        this.addSibling(replacementChannel);
        this.removeSiblings();

        return replacementChannel;
    }

    public static class Initializer implements ChannelInitializer {

        @Override
        public Channel setUpOutput(ExecutionTask executionTask, int index) {
            final Channel existingOutputChannel = executionTask.getOutputChannel(index);
            if (existingOutputChannel == null) {
                return new StreamChannel(executionTask, index);
            } else if (existingOutputChannel instanceof StreamChannel) {
                return existingOutputChannel;
            } else if (existingOutputChannel instanceof CollectionChannel) {
                // That's fine as well. The decision to use a StreamChannel has been overridden.
                return existingOutputChannel;
            } else {
                throw new IllegalStateException();
            }
        }

        @Override
        public void setUpInput(Channel channel, ExecutionTask executionTask, int index) {
            assert channel instanceof StreamChannel;
            channel.addConsumer(executionTask, index);
        }

        @Override
        public boolean isReusable() {
            return false;
        }

        @Override
        public boolean isInternal() {
            return true;
        }

    }

    public static class Executor implements ChannelExecutor {

        private Stream<?> stream;

        private boolean isMarkedForInstrumentation;

        private long cardinality = -1;

        public Executor(boolean isMarkedForInstrumentation) {
            this.isMarkedForInstrumentation = isMarkedForInstrumentation;
        }

        @Override
        public void acceptStream(Stream<?> stream) {
            this.stream = stream;
            if (this.isMarkedForInstrumentation) {
                this.stream = this.stream.filter(dataQuantum -> {
                    this.cardinality += 1;
                    return true;
                })
                .onClose(() -> this.cardinality++);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public Stream<?> provideStream() {
            return this.stream;
        }

        @Override
        public void acceptCollection(Collection<?> collection) {
            if (this.isMarkedForInstrumentation) {
                this.cardinality = collection.size();
            }
            this.stream = collection.stream();
        }

        @Override
        @SuppressWarnings("unchecked")
        public Collection<?> provideCollection() {
            throw new RuntimeException("Not available for this channel type.");
        }

        @Override
        public boolean canProvideCollection() {
            return false;
        }

        @Override
        public long getCardinality() throws RheemException {
            assert this.isMarkedForInstrumentation;
            return this.cardinality;
        }

        @Override
        public void markForInstrumentation() {
            this.isMarkedForInstrumentation = true;
        }
    }

}
