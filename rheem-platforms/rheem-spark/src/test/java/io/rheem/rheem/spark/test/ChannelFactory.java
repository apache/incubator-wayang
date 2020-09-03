package io.rheem.rheem.spark.test;

import org.junit.Before;
import io.rheem.rheem.core.api.Configuration;
import io.rheem.rheem.core.plan.executionplan.Channel;
import io.rheem.rheem.core.platform.ChannelDescriptor;
import io.rheem.rheem.core.util.RheemCollections;
import io.rheem.rheem.java.channels.CollectionChannel;
import io.rheem.rheem.spark.channels.RddChannel;
import io.rheem.rheem.spark.execution.SparkExecutor;

import java.util.Collection;

import static org.mockito.Mockito.mock;

/**
 * Utility to create {@link Channel}s in tests.
 */
public class ChannelFactory {

    private static SparkExecutor sparkExecutor;

    @Before
    public void setUp() {
        sparkExecutor = mock(SparkExecutor.class);
    }

    public static RddChannel.Instance createRddChannelInstance(ChannelDescriptor rddChannelDescriptor, Configuration configuration) {
        return (RddChannel.Instance) rddChannelDescriptor
                .createChannel(null, configuration)
                .createInstance(sparkExecutor, null, -1);
    }

    public static RddChannel.Instance createRddChannelInstance(Configuration configuration) {
        return createRddChannelInstance(RddChannel.UNCACHED_DESCRIPTOR, configuration);
    }

    public static RddChannel.Instance createRddChannelInstance(Collection<?> data,
                                                               SparkExecutor sparkExecutor,
                                                               Configuration configuration) {
        RddChannel.Instance instance = createRddChannelInstance(configuration);
        instance.accept(sparkExecutor.sc.parallelize(RheemCollections.asList(data)), sparkExecutor);
        return instance;
    }

    public static CollectionChannel.Instance createCollectionChannelInstance(Configuration configuration) {
        return (CollectionChannel.Instance) CollectionChannel.DESCRIPTOR
                .createChannel(null, configuration)
                .createInstance(sparkExecutor, null, -1);
    }

    public static CollectionChannel.Instance createCollectionChannelInstance(Collection<?> colleciton, Configuration configuration) {
        CollectionChannel.Instance instance = createCollectionChannelInstance(configuration);
        instance.accept(colleciton);
        return instance;
    }

}
