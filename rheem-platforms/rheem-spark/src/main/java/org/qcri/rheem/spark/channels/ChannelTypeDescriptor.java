package org.qcri.rheem.spark.channels;

import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.spark.platform.SparkExecutor;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Describes a certain type of {@link Channel}.
 */
public class ChannelTypeDescriptor {

    private final ChannelInitializer initializer;

    private final BiFunction<Channel, SparkExecutor, ChannelExecutor> executorFactory;

    public ChannelTypeDescriptor(ChannelInitializer initializer, BiFunction<Channel, SparkExecutor, ChannelExecutor> executorFactory) {
        this.initializer = initializer;
        this.executorFactory = executorFactory;
    }

    public ChannelInitializer getInitializer() {
        return this.initializer;
    }

    public BiFunction<Channel, SparkExecutor, ChannelExecutor> getExecutorFactory() {
        return this.executorFactory;
    }
}
