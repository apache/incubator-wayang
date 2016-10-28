package org.qcri.rheem.core.plan.rheemplan;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;

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
    default Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.empty();
    }

    /**
     * Retrieve a {@link CardinalityEstimator} tied specifically to this instance.
     *
     * @param outputIndex for the output described by the {@code cardinalityEstimator}
     * @return the {@link CardinalityEstimator} or {@code null} if none exists
     */
    CardinalityEstimator getCardinalityEstimator(int outputIndex);

    /**
     * Tie a specific {@link CardinalityEstimator} to this instance.
     *
     * @param outputIndex          for the output described by the {@code cardinalityEstimator}
     * @param cardinalityEstimator the {@link CardinalityEstimator}
     */
    void setCardinalityEstimator(int outputIndex, CardinalityEstimator cardinalityEstimator);

    /**
     * Tells whether this instance is auxiliary, i.e., it support some non-auxiliary operators.
     *
     * @return whether this instance is auxiliary
     */
    boolean isAuxiliary();

    /**
     * Tell whether this instance is auxiliary, i.e., it support some non-auxiliary operators.
     *
     * @param isAuxiliary whether this instance is auxiliary
     */
    void setAuxiliary(boolean isAuxiliary);

}
