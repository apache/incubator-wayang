package org.qcri.rheem.core.platform;

import org.slf4j.LoggerFactory;

import java.util.OptionalLong;

/**
 * Template for {@link ChannelInstance} implementations.
 */
public abstract class AbstractChannelInstance extends ExecutionResourceTemplate implements ChannelInstance {

    private OptionalLong measuredCardinality = OptionalLong.empty();

    /**
     * Creates a new instance and registers it with its {@link Executor}.
     *
     * @param executor that maintains this instance
     */
    protected AbstractChannelInstance(Executor executor) {
        super(executor);
    }

    @Override
    public OptionalLong getMeasuredCardinality() {
        return this.measuredCardinality;
    }

    @Override
    public void setMeasuredCardinality(long cardinality) {
        this.measuredCardinality.ifPresent(oldCardinality ->
                LoggerFactory.getLogger(this.getClass()).warn(
                        "Replacing existing measured cardinality of {} with {} (was {}).",
                        this.getChannel(),
                        cardinality,
                        oldCardinality
                )
        );
        this.measuredCardinality = OptionalLong.of(cardinality);
    }

    @Override
    public String toString() {
        return "*" + this.getChannel().toString();
    }
}
