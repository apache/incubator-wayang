package org.qcri.rheem.java.channels;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * {@link Channel} between two {@link JavaExecutionOperator}s using a {@link Stream}.
 */
public class StreamChannel extends Channel {

    private static final boolean IS_REUSABLE = false;

    private static final boolean IS_INTERNAL = true;

    public static final ChannelDescriptor DESCRIPTOR = new ChannelDescriptor(StreamChannel.class, IS_REUSABLE, IS_REUSABLE, IS_REUSABLE & !IS_INTERNAL);

    protected StreamChannel(ChannelDescriptor descriptor) {
        super(descriptor);
        assert descriptor == DESCRIPTOR;
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

    public Channel exchangeWith(ChannelDescriptor descriptor) {
        throw new UnsupportedOperationException();
//        ChannelInitializer channelInitializer = JavaPlatform.getInstance()
//                .getChannelManager()
//                .getChannelInitializer(descriptor);
//        final ExecutionTask producer = this.producer;
//        final int outputIndex = this.producer.removeOutputChannel(this);
//        final Channel replacementChannel = channelInitializer.setUpOutput(descriptor, producer, outputIndex);
//
//        for (ExecutionTask consumer : this.consumers) {
//            int inputIndex = consumer.removeInputChannel(this);
//            channelInitializer.setUpInput(replacementChannel, consumer, inputIndex);
//        }
//
//        if (this.isMarkedForInstrumentation()) {
//            replacementChannel.markForInstrumentation();
//        }
//        this.addSibling(replacementChannel);
//        this.removeSiblings();
//
//        return replacementChannel;
    }

    public static class Initializer implements JavaChannelInitializer {

        @Override
        public Tuple<Channel, Channel> setUpOutput(ChannelDescriptor descriptor, OutputSlot<?> outputSlot) {
            StreamChannel channel = new StreamChannel(descriptor);
            return new Tuple<>(channel, channel);
        }

        @Override
        public Channel setUpOutput(ChannelDescriptor descriptor, Channel source) {
            assert descriptor == StreamChannel.DESCRIPTOR;
            final JavaChannelInitializer channelInitializer = this.getChannelManager().getChannelInitializer(source.getDescriptor());
            return channelInitializer.provideStreamChannel(source);
        }

        @Override
        public StreamChannel provideStreamChannel(Channel channel) {
            return (StreamChannel) channel;
        }
    }

    public static class Executor implements ChannelExecutor {

        private Stream<?> stream;

        private boolean isMarkedForInstrumentation;

        // In principle, we could use Stream#onClose() to make sure that we really counted the cardinality (so as to
        // detect, when the cardinality is 0 because the #stream has not been fully executed for whatever reason).
        // However, this would require to call Stream#close() on all methods.
        private long cardinality = 0;

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
                });
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

        @Override
        public boolean ensureExecution() {
            assert this.stream != null;
            // We cannot ensure execution. For this purpose, we would need a CollectionChannel.
            return false;
        }
    }

}
