package org.qcri.rheem.spark.channels;

import org.apache.spark.api.java.JavaRDDLike;
import org.qcri.rheem.core.plan.executionplan.Channel;

import java.util.stream.Stream;

/**
 * Defines execution logic to handle a {@link Channel}.
 */
public interface ChannelExecutor {

    /**
     * Accept the result of the producer of a {@link Channel}.
     *
     * @param rdd the producer's result
     */
    void acceptRdd(JavaRDDLike rdd);

    /**
     * Provide the producer's result to a consumer.
     *
     * @return the producer's result
     */
    JavaRDDLike provideRdd();

    /**
     * Releases resources held by this instance.
     */
    void dispose();
}
