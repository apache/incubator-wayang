package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.InputSlot;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.OutputSlot;

import java.util.Map;

public interface CardinalityPusher {

    CardinalityEstimate push(RheemContext rheemContext, CardinalityEstimate... inputEstimates);

    abstract class WithCache implements CardinalityPusher {

        /**
         * {@link OutputSlot} whose output cardinality is being estimated.
         */
        protected final Map<OutputSlot<?>, CardinalityEstimate> estimateCache;

        public WithCache(Map<OutputSlot<?>, CardinalityEstimate> estimateCache) {
            this.estimateCache = estimateCache;
        }

    }

}
