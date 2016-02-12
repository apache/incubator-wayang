package org.qcri.rheem.spark.channels;

import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;

import java.util.function.Function;

/**
 * Describes a certain type of {@link Channel}.
 */
public class ChannelTypeDescriptor<T extends Channel> {

    private final ChannelInitializer<T> initializer;

    private final Function<Channel, ChannelExecutor> executorFactory;

    public ChannelTypeDescriptor(ChannelInitializer<T> initializer, Function<Channel, ChannelExecutor> executorFactory) {
        this.initializer = initializer;
        this.executorFactory = executorFactory;
    }

    public ChannelInitializer<T> getInitializer() {
        return this.initializer;
    }

    public Function<Channel, ChannelExecutor> getExecutorFactory() {
        return this.executorFactory;
    }
}
