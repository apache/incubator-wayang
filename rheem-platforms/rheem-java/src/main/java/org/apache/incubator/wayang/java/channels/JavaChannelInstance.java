package org.apache.incubator.wayang.java.channels;

import org.apache.incubator.wayang.core.plan.executionplan.Channel;
import org.apache.incubator.wayang.core.platform.ChannelInstance;

import java.util.stream.Stream;

/**
 * Defines execution logic to handle a {@link Channel}.
 */
public interface JavaChannelInstance extends ChannelInstance {

    /**
     * Provide the producer's result to a consumer.
     *
     * @return the producer's result
     */
    <T> Stream<T> provideStream();

}
