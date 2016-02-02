package org.qcri.rheem.core.optimizer.costs;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.OutputSlot;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@link CardinalityEstimator} implementation that can have multiple ways of calculating a {@link CardinalityEstimate}.
 */
public class AggregatingCardinalityEstimator extends CardinalityEstimator.WithCache {

    private final List<CardinalityEstimator> alternativeEstimators;

    public AggregatingCardinalityEstimator(List<CardinalityEstimator> alternativeEstimators,
                                           OutputSlot<?> targetOutput,
                                           Map<OutputSlot<?>, CardinalityEstimate> cache) {
        super(targetOutput, cache);
        Validate.isTrue(!alternativeEstimators.isEmpty());
        this.alternativeEstimators = new ArrayList<>(alternativeEstimators);
    }

    @Override
    public CardinalityEstimate calculateEstimate(RheemContext rheemContext, CardinalityEstimate... inputEstimates) {
        // Simply use the estimate with the highest correctness probability.
        // TODO: Check if this is a good way. There are other palpable approaches (e.g., weighted average).
        return this.alternativeEstimators.stream()
                .map(alternativeEstimator -> alternativeEstimator.estimate(rheemContext, inputEstimates))
                .sorted((estimate1, estimate2) ->
                        Double.compare(estimate2.getCorrectnessProbability(), estimate1.getCorrectnessProbability()))
                .findFirst()
                .orElseThrow(IllegalStateException::new);
    }
}
