package org.qcri.rheem.java.test;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.StreamChannel;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * Utility to create {@link Channel}s in tests.
 */
public class ChannelFactory {

    public static StreamChannel.Instance createStreamChannelInstance(Configuration configuration) {
        return (StreamChannel.Instance) StreamChannel.DESCRIPTOR.createChannel(null, configuration).createInstance();
    }

    public static StreamChannel.Instance createStreamChannelInstance(Stream<?> stream, Configuration configuration) {
        StreamChannel.Instance instance = createStreamChannelInstance(configuration);
        instance.accept(stream);
        return instance;
    }

    public static CollectionChannel.Instance createCollectionChannelInstance(Configuration configuration) {
        return (CollectionChannel.Instance) CollectionChannel.DESCRIPTOR.createChannel(null, configuration).createInstance();
    }

    public static CollectionChannel.Instance createCollectionChannelInstance(Collection<?> colleciton, Configuration configuration) {
        CollectionChannel.Instance instance = createCollectionChannelInstance(configuration);
        instance.accept(colleciton);
        return instance;
    }

}
