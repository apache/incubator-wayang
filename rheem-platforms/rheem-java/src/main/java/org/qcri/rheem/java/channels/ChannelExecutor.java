package org.qcri.rheem.java.channels;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.platform.ChannelInstance;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * Defines execution logic to handle a {@link Channel}.
 */
public interface ChannelExecutor extends ChannelInstance {

    /**
     * Accept the result of the producer of a {@link Channel}.
     *
     * @param stream the producer's result
     */
    void acceptStream(Stream<?> stream);

    /**
     * Accept the result of the producer of a {@link Channel}.
     *
     * @param collection the producer's result
     */
    void acceptCollection(Collection<?> collection);

    /**
     * Provide the producer's result to a consumer.
     *
     * @return the producer's result
     */
    <T> Stream<T> provideStream();

    /**
     * Provide the producer's result to a consumer. If this option is available is determined via {@link #canProvideCollection()}.
     *
     * @return the producer's result
     */
    <T> Collection<T> provideCollection();

    /**
     * @return whether this instance can provide its result via {@link #provideCollection()}
     */
    boolean canProvideCollection();

    /**
     * Declares that this instance should be instrumented in order to collect statistics during execution.
     *
     * @see #getMeasuredCardinality()
     */
    void markForInstrumentation();

    /**
     * Request this instance to pull the data for its {@link Channel} if this has not happened yet.
     *
     * @return whether the execution really took place
     */
    boolean ensureExecution();

}
