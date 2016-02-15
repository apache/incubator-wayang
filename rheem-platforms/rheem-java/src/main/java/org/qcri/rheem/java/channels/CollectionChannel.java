package org.qcri.rheem.java.channels;

import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link Channel} between two {@link JavaExecutionOperator}s using an intermediate {@link Collection}.
 */
public class CollectionChannel extends Channel {

    protected CollectionChannel(ExecutionTask producer, int outputIndex) {
        super(producer, outputIndex);
    }

    @Override
    public boolean isReusable() {
        return true;
    }

    public static class Initializer implements ChannelInitializer<CollectionChannel> {

        @Override
        public CollectionChannel setUpOutput(ExecutionTask executionTask, int index) {
            // TODO: We might need to add a "collector" operator or the like because this channel might introduce overhead.
            // Then we might also get rid of the ChannelExecutors.
            final Channel existingOutputChannel = executionTask.getOutputChannel(index);
            if (existingOutputChannel == null) {
                return new CollectionChannel(executionTask, index);
            } else if (existingOutputChannel instanceof CollectionChannel) {
                return (CollectionChannel) existingOutputChannel;
            } else {
                throw new IllegalStateException();
            }
        }

        @Override
        public void setUpInput(CollectionChannel collectionChannel, ExecutionTask executionTask, int index) {
            collectionChannel.addConsumer(executionTask, index);
        }

        @Override
        public boolean isReusable() {
            return true;
        }
    }

    public static class Executor implements ChannelExecutor {

        private Collection<?> collection;

        @Override
        public void acceptStream(Stream<?> stream) {
            this.collection = stream.collect(Collectors.toList());
        }

        @Override
        public Stream<?> provideStream() {
            return this.collection.stream();
        }
    }
}
