package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.InputSlot;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.OutputSlot;

import java.util.Map;

/**
 * Computes {@link CardinalityEstimate}s.
 * <p>To estimate the cardinality of an {@link Operator}'s {@link OutputSlot} given its {@link InputSlot}s' cardinalities
 * is the job of this estimator.</p>
 */
public interface CardinalityEstimator {

    CardinalityEstimate estimate(RheemContext rheemContext, CardinalityEstimate... inputEstimates);

    abstract class WithCache implements CardinalityEstimator {

        /**
         * {@link OutputSlot} whose output cardinality is being estimated.
         */
        protected final OutputSlot<?> targetOutput;

        protected final Map<OutputSlot<?>, CardinalityEstimate> estimateCache;

        public WithCache(OutputSlot<?> targetOutput, Map<OutputSlot<?>, CardinalityEstimate> estimateCache) {
            this.targetOutput = targetOutput;
            this.estimateCache = estimateCache;
        }

        @Override
        public CardinalityEstimate estimate(RheemContext rheemContext, CardinalityEstimate... inputEstimates) {
            if (estimateCache != null) {
                final CardinalityEstimate cachedEstimate = this.estimateCache.get(this.targetOutput);
                if (cachedEstimate != null) {
                    return cachedEstimate;
                }
            }
            CardinalityEstimate cardinalityEstimate = this.calculateEstimate(rheemContext, inputEstimates);
            if (this.estimateCache != null) {
                this.estimateCache.put(this.targetOutput, cardinalityEstimate);
            }
            return cardinalityEstimate;
        }

        /**
         * Do the actual estimation.
         */
        protected abstract CardinalityEstimate calculateEstimate(RheemContext rheemContext, CardinalityEstimate... inputEstimates);
    }

}
