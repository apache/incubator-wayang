package org.qcri.rheem.core.plan.rheemplan;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.FallbackCardinalityEstimator;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Indivisible {@link Operator} that is not containing other {@link Operator}s.
 */
public interface ElementaryOperator extends ActualOperator {

    /**
     * Provide a {@link CardinalityEstimator} for the {@link OutputSlot} at {@code outputIndex}.
     *
     * @param outputIndex   index of the {@link OutputSlot} for that the {@link CardinalityEstimator} is requested
     * @param configuration if the {@link CardinalityEstimator} depends on further ones, use this to obtain the latter
     * @return an {@link Optional} that might provide the requested instance
     */
    default Optional<CardinalityEstimator> getCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        LoggerFactory.getLogger(this.getClass()).warn("Use fallback cardinality estimator for {}.", this);
        return Optional.of(new FallbackCardinalityEstimator());
    }

}
