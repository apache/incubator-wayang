package org.qcri.rheem.java.channels;

import org.qcri.rheem.core.plan.executionplan.Channel;

import java.util.stream.Stream;

/**
 * Defines execution logic to handle a {@link Channel}.
 */
public interface ChannelExecutor {

    /**
     * Accept the result of the producer of a {@link Channel}.
     *
     * @param stream the producer's result
     */
    void acceptStream(Stream<?> stream);

    /**
     * Provide the producer's result to a consumer.
     *
     * @return the producer's result
     */
    Stream<?> provideStream();

}
