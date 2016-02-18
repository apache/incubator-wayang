package org.qcri.rheem.java.channels;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.executionplan.Channel;

import java.util.Collection;
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
     * @return the cardinality measured by this instance or {@code -1} if for some reason no cardinality was measured
     * @throws RheemException if the corresponding {@link Channel} is not marked for instrumentation
     */
    long getCardinality() throws RheemException;

    /**
     * Declares that this instance should be instrumented in order to collect statistics during execution.
     *
     * @see #getCardinality()
     */
    void markForInstrumentation();

}
