package org.qcri.rheem.core.optimizer.cardinality;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.OutputSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Provides {@link CardinalityEstimator}s for {@link OutputSlot}s and {@link CardinalityPusher}s for {@link Operator}s.
 */
public class CardinalityEstimationProvider {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final CardinalityEstimationProvider parent;

    private final Map<OutputSlot<?>, CardinalityEstimator> customCardinalityEstimators = new HashMap<>();

    private Map<OutputSlot<?>, CardinalityEstimate> cardinalityEstimateCache;

    public CardinalityEstimationProvider() {
        this(null);
    }

    private CardinalityEstimationProvider(CardinalityEstimationProvider parent) {
        this.parent = parent;
    }

    public CardinalityEstimationProvider createChild() {
        return new CardinalityEstimationProvider(this);
    }

    public CardinalityEstimator provideEstimator(OutputSlot<?> outputSlot) {
        Validate.notNull(outputSlot);

        // Look for a custom answer.
        final CardinalityEstimator cardinalityEstimator = this.customCardinalityEstimators.get(outputSlot);
        if (cardinalityEstimator != null) {
            return cardinalityEstimator;
        }

        // If present, delegate request to parent.
        if (this.parent != null) {
            return this.parent.provideEstimator(outputSlot);
        }

        // Try to resolve from operator.
        final Operator operator = outputSlot.getOwner();
        Validate.notNull(this.cardinalityEstimateCache != null, "Cache for CardinalityEstimates not set up!");
        final Optional<CardinalityEstimator> optionalEstimator =
                operator.getCardinalityEstimator(outputSlot.getIndex(), this.cardinalityEstimateCache);
        return optionalEstimator.orElseGet(() -> {
                this.logger.warn("Create fallback estimator for %s.", outputSlot);
                return new FallbackCardinalityEstimator(outputSlot, this.cardinalityEstimateCache);
        });

    }

    /**
     * Assumes with a confidence of 50% that the output cardinality will be somewhere between 1 and the product of
     * all 10*input estimates.
     */
    public static class FallbackCardinalityEstimator extends CardinalityEstimator.WithCache {

        public FallbackCardinalityEstimator(OutputSlot<?> targetOutput, Map<OutputSlot<?>, CardinalityEstimate> estimateCache) {
            super(targetOutput, estimateCache);
        }

        @Override
        protected CardinalityEstimate calculateEstimate(RheemContext rheemContext, CardinalityEstimate... inputEstimates) {
            double probability = .5d;
            long upperEstimate = 1L;
            for (CardinalityEstimate inputEstimate : inputEstimates) {
                probability *= inputEstimate.getCorrectnessProbability();
                upperEstimate *= 10 * inputEstimate.getUpperEstimate();
                if (upperEstimate < 0L) {
                    upperEstimate = Long.MAX_VALUE;
                }
            }
            return new CardinalityEstimate(1L, upperEstimate, probability);
        }

    }

}
