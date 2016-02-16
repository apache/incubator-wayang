package org.qcri.rheem.java.channels;

import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.util.stream.Stream;

/**
 * {@link Channel} between two {@link JavaExecutionOperator}s using a {@link Stream}.
 */
public class StreamChannel extends Channel {

    protected StreamChannel(ExecutionTask producer, int outputIndex) {
        super(producer, outputIndex);
    }

    @Override
    public boolean isReusable() {
        return false;
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

        @Override
        public void acceptStream(Stream<?> stream) {
            this.stream = stream;
        }

        @Override
        public Stream<?> provideStream() {
            return this.stream;
        }
    }
}
