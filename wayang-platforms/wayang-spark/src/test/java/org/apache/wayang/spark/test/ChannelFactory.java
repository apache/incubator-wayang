package org.apache.wayang.spark.test;

import org.junit.Before;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;

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
        instance.accept(sparkExecutor.sc.parallelize(WayangCollections.asList(data)), sparkExecutor);
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
