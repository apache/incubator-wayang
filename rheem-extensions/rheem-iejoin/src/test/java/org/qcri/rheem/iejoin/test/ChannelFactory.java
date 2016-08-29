package org.qcri.rheem.iejoin.test;

import org.junit.Before;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.execution.SparkExecutor;

import java.util.Collection;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;

/**
 * Utility to create {@link Channel}s in tests.
 */
public class ChannelFactory {

    private static SparkExecutor sparkExecutor;

    private static JavaExecutor javaExecutor;

    @Before
    public void setUp() {
        sparkExecutor = mock(SparkExecutor.class);
        javaExecutor = mock(JavaExecutor.class);
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

    public static StreamChannel.Instance createStreamChannelInstance(Configuration configuration) {
        return (StreamChannel.Instance) StreamChannel.DESCRIPTOR
                .createChannel(null, configuration)
                .createInstance(javaExecutor, null, -1);
    }

    public static StreamChannel.Instance createStreamChannelInstance(Stream<?> stream, Configuration configuration) {
        StreamChannel.Instance instance = createStreamChannelInstance(configuration);
        instance.accept(stream);
        return instance;
    }

    public static CollectionChannel.Instance createCollectionChannelInstance(Configuration configuration) {
        return (CollectionChannel.Instance) CollectionChannel.DESCRIPTOR
                .createChannel(null, configuration)
                .createInstance(javaExecutor, null, -1);
    }

    public static CollectionChannel.Instance createCollectionChannelInstance(Collection<?> collection, Configuration configuration) {
        CollectionChannel.Instance instance = createCollectionChannelInstance(configuration);
        instance.accept(collection);
        return instance;
    }

}
