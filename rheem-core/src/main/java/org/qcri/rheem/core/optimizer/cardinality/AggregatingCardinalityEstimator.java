package org.qcri.rheem.core.optimizer.cardinality;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.optimizer.OptimizationContext;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link CardinalityEstimator} implementation that can have multiple ways of calculating a {@link CardinalityEstimate}.
 */
public class AggregatingCardinalityEstimator implements CardinalityEstimator {

    private final List<CardinalityEstimator> alternativeEstimators;

    public AggregatingCardinalityEstimator(List<CardinalityEstimator> alternativeEstimators) {
        Validate.isTrue(!alternativeEstimators.isEmpty());
        this.alternativeEstimators = new ArrayList<>(alternativeEstimators);
    }

    @Override
    public CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates) {
        // Simply use the estimate with the highest correctness probability.
        // TODO: Check if this is a good way. There are other palpable approaches (e.g., weighted average).
        return this.alternativeEstimators.stream()
                .map(alternativeEstimator -> alternativeEstimator.estimate(optimizationContext, inputEstimates))
                .sorted((estimate1, estimate2) ->
                        Double.compare(estimate2.getCorrectnessProbability(), estimate1.getCorrectnessProbability()))
                .findFirst()
                .orElseThrow(IllegalStateException::new);
    }
}
