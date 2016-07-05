package org.qcri.rheem.java.test;

import org.junit.Before;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.Collection;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;

/**
 * Utility to create {@link Channel}s in tests.
 */
public class ChannelFactory {

    private static JavaExecutor executor;

    @Before
    public void setUp() {
        executor = mock(JavaExecutor.class);
    }

    public static StreamChannel.Instance createStreamChannelInstance(Configuration configuration) {
        return (StreamChannel.Instance) StreamChannel.DESCRIPTOR
                .createChannel(null, configuration)
                .createInstance(executor, null, -1);
    }

    public static StreamChannel.Instance createStreamChannelInstance(Stream<?> stream, Configuration configuration) {
        StreamChannel.Instance instance = createStreamChannelInstance(configuration);
        instance.accept(stream);
        return instance;
    }

    public static CollectionChannel.Instance createCollectionChannelInstance(Configuration configuration) {
        return (CollectionChannel.Instance) CollectionChannel.DESCRIPTOR
                .createChannel(null, configuration)
                .createInstance(executor, null, -1);
    }

    public static CollectionChannel.Instance createCollectionChannelInstance(Collection<?> collection, Configuration configuration) {
        CollectionChannel.Instance instance = createCollectionChannelInstance(configuration);
        instance.accept(collection);
        return instance;
    }

}
