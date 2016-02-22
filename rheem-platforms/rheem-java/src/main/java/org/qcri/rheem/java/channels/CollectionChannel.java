package org.qcri.rheem.java.channels;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link Channel} between two {@link JavaExecutionOperator}s using an intermediate {@link Collection}.
 */
public class CollectionChannel extends Channel {

    private static final boolean IS_REUSABLE = true;

    private static final boolean IS_INTERNAL = true;

    protected CollectionChannel(ExecutionTask producer, int outputIndex) {
        super(producer, outputIndex);
    }

    private CollectionChannel(CollectionChannel parent) {
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
    public CollectionChannel copy() {
        return new CollectionChannel(this);
    }

    public static class Initializer implements ChannelInitializer {

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
                throw new IllegalStateException(String.format(
                        "Expected %s, encountered %s.", CollectionChannel.class.getSimpleName(), existingOutputChannel
                ));
            }
        }

        @Override
        public void setUpInput(Channel channel, ExecutionTask executionTask, int index) {
            assert channel instanceof CollectionChannel;
            channel.addConsumer(executionTask, index);
        }

        @Override
        public boolean isReusable() {
            return true;
        }

        @Override
        public boolean isInternal() {
            return false;
        }
    }

    public static class Executor implements ChannelExecutor {

        private Collection<?> collection;

        private long count = -1;

        private boolean isMarkedForInstrumentation;

        public Executor(boolean isMarkedForInstrumentation) {
            this.isMarkedForInstrumentation = isMarkedForInstrumentation;
        }

        @Override
        public void acceptStream(Stream<?> stream) {
            this.collection = stream.collect(Collectors.toList());
            this.count = this.collection.size();
        }

        @Override
        public void acceptCollection(Collection<?> collection) {
            this.collection = collection;
            this.count = this.collection.size();
        }

        @Override
        @SuppressWarnings("unchecked")
        public Collection<?> provideCollection() {
            return this.collection;
        }

        @Override
        public boolean canProvideCollection() {
            return true;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Stream<?> provideStream() {
            return this.collection.stream();
        }

        @Override
        public long getCardinality() throws RheemException {
            assert this.isMarkedForInstrumentation;
            return this.count;
        }

        @Override
        public void markForInstrumentation() {
            this.isMarkedForInstrumentation = true;
        }

        @Override
        public boolean ensureExecution() {
            assert this.collection != null;
            return true;
        }
    }
}
