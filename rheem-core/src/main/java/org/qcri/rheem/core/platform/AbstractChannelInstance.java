package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.api.exception.RheemException;
import org.slf4j.LoggerFactory;

import java.util.OptionalLong;

/**
 * Template for {@link ChannelInstance} implementations.
 */
public abstract class AbstractChannelInstance implements ChannelInstance {

    private OptionalLong measuredCardinality = OptionalLong.empty();

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
    public void release() throws RheemException {
        try {
            this.tryToRelease();
        } catch (RheemException e) {
            throw e;
        } catch (Exception e) {
            throw new RheemException(String.format("Could not release %s.", this.getChannel()), e);
        }
    }

    /**
     * Override this method to implement {@link #release()} without taking care of {@link Exception}s.
     */
    protected void tryToRelease() throws Exception { };
}
